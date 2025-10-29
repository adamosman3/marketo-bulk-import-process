/**
 * Cloudflare Worker for Secure Marketo & Gemini API Operations
 *
 * This worker securely handles:
 * 1. Marketo Authentication (using environment variables)
 * 2. Marketo Bulk Import Jobs (/api/import)
 * 3. Marketo Standard Lead Insertion (/api/single-lead)
 * 4. Marketo Field Metadata Fetching (/api/fields)
 * 5. Marketo Program Search (/api/programs)
 * 6. Gemini API for Unstructured Data Parsing (/api/parse-leads)
 * 7. Gemini API for Job Title Description (/api/describe-title)
 */

// --- CORS & Utility Functions ---

// Define allowed origins. For production, lock this down.
// const ALLOWED_ORIGINS = ['https://your-pages-project.pages.dev', 'http://localhost:3000'];
const ALLOWED_ORIGINS = '*'; // Use '*' for broad development access

/**
 * Handles CORS preflight (OPTIONS) requests and adds CORS headers to responses.
 * @param {Request} request - The incoming request.
 * @param {Response} response - The outgoing response.
 * @returns {Response} A response with appropriate CORS headers.
 */
function handleCors(request, response) {
    const origin = request.headers.get('Origin');
    
    // if (origin && ALLOWED_ORIGINS.includes(origin)) {
    //     response.headers.set('Access-Control-Allow-Origin', origin);
    // } else if (ALLOWED_ORIGINS === '*') {
        response.headers.set('Access-Control-Allow-Origin', '*');
    // }

    response.headers.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    response.headers.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    response.headers.set('Access-Control-Max-Age', '86400'); // 24 hours

    return response;
}

/**
 * Creates a standard JSON response with CORS headers.
 * @param {object} data - The JSON data to send.
 * @param {number} status - The HTTP status code.
 * @returns {Response} A JSON response with CORS.
 */
function jsonResponse(data, status = 200) {
    const response = new Response(JSON.stringify(data), {
        status: status,
        headers: { 'Content-Type': 'application/json' },
    });
    // Apply CORS headers to *all* responses
    return handleCors(new Request(self.location, { headers: {} }), response);
}

/**
 * Creates a standard error response.
 * @param {string} message - The error message.
 * @param {number} status - The HTTP status code.
 * @returns {Response} A JSON error response.
 */
function errorResponse(message, status = 500) {
    return jsonResponse({ status: 'error', message: message }, status);
}

/**
 * Exponential backoff delay for polling.
 * @param {number} attempt - The current retry attempt number.
 * @returns {Promise<void>}
 */
function pollDelay(attempt) {
    const delay = Math.pow(2, attempt) * 1000; // 1s, 2s, 4s, 8s...
    return new Promise(resolve => setTimeout(resolve, Math.min(delay, 30000))); // Max 30s delay
}


// --- Main Worker Fetch Handler ---

export default {
    async fetch(request, env) {
        // Handle CORS preflight (OPTIONS) requests
        if (request.method === 'OPTIONS') {
            const response = new Response(null, { status: 204 });
            return handleCors(request, response);
        }

        const url = new URL(request.url);

        try {
            // --- API ROUTING ---
            switch (url.pathname) {
                case '/api/import':
                    return await handleBulkImport(request, env);
                case '/api/single-lead':
                    return await handleSingleLead(request, env);
                case '/api/fields':
                    return await handleFieldFetch(request, env);
                case '/api/programs':
                    return await handleProgramSearch(request, env);
                case '/api/parse-leads':
                    return await handleGeminiParse(request, env);
                case '/api/describe-title':
                    return await handleGeminiDescribe(request, env);
                default:
                    return errorResponse('Not Found. This Worker only responds to /api/* routes.', 404);
            }
        } catch (error) {
            console.error('Unhandled Worker Error:', error.message, error.stack);
            return errorResponse(`Internal Server Error: ${error.message}`, 500);
        }
    },
};

// --- Marketo Authentication ---

/**
 * Retrieves a Marketo Access Token using secure environment variables.
 * @param {object} env - The worker environment (contains secrets).
 * @returns {Promise<string>} The Marketo access token.
 */
async function getAccessToken(env) {
    const { MARKETO_ENDPOINT, MARKETO_CLIENT_ID, MARKETO_CLIENT_SECRET } = env;
    if (!MARKETO_ENDPOINT || !MARKETO_CLIENT_ID || !MARKETO_CLIENT_SECRET) {
        throw new Error('Marketo environment variables are not set.');
    }
    
    const url = `${MARKETO_ENDPOINT}/identity/oauth/token?grant_type=client_credentials&client_id=${MARKETO_CLIENT_ID}&client_secret=${MARKETO_CLIENT_SECRET}`;
    
    const response = await fetch(url, { method: 'GET' });
    const json = await response.json();
    
    if (!response.ok || !json.access_token) {
        throw new Error('Failed to retrieve Marketo access token: ' + JSON.stringify(json));
    }
    
    return json.access_token;
}

// --- API Endpoint Handlers ---

/**
 * 1. Handles Bulk Import Job: Creates job, uploads CSV, polls for status.
 * /api/import (POST)
 */
async function handleBulkImport(request, env) {
    const body = await request.json();
    const { csvContent, lookupField, programName } = body;
    if (!csvContent || !lookupField) {
        return errorResponse('Missing csvContent or lookupField.', 400);
    }
    
    const token = await getAccessToken(env);
    
    // Step 1 & 2: Create Job and Upload Data
    const { batchId, status } = await createImportJobAndUploadData(env, token, csvContent, lookupField);
    if (status !== 'Queued' && status !== 'Processing') {
        throw new Error(`Marketo job failed at upload stage. Status: ${status}`);
    }
    
    // Step 3: Poll for Status
    const finalStatus = await pollJobStatus(env, token, batchId);
    
    return jsonResponse({
        status: finalStatus.status,
        jobId: batchId,
        programName: programName || 'N/A',
        successRows: finalStatus.numOfRowsProcessed || 0,
        errorRows: finalStatus.numOfRowsFailed || 0,
        warnings: finalStatus.warnings || [],
    });
}

/**
 * 2. Handles Single Lead Insertion: Uses standard /rest/v1/leads.json API.
 * /api/single-lead (POST)
 */
async function handleSingleLead(request, env) {
    const body = await request.json();
    const { programName, leads } = body;
    if (!leads || !Array.isArray(leads) || leads.length === 0) {
        return errorResponse('Missing leads array.', 400);
    }
    
    const token = await getAccessToken(env);
    const url = `${env.MARKETO_ENDPOINT}/rest/v1/leads.json`;

    const payload = {
        action: "createOrUpdate",
        lookupField: "email",
        input: leads
    };
    
    // Add programName if provided (for program member status logic)
    if (programName) {
        payload.programName = programName;
    }

    const response = await fetch(url, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify(payload)
    });
    
    const result = await response.json();
    
    if (!response.ok || !result.success) {
        return errorResponse(`Marketo API Error: ${JSON.stringify(result.errors)}`, 502);
    }
    
    return jsonResponse({
        status: 'success',
        result: result.result
    });
}

/**
 * 3. Handles Field Metadata Fetching: Calls /rest/v1/leads/describe.json.
 * /api/fields (GET)
 */
async function handleFieldFetch(request, env) {
    const token = await getAccessToken(env);
    const url = `${env.MARKETO_ENDPOINT}/rest/v1/leads/describe.json`;
    
    const response = await fetch(url, {
        method: 'GET',
        headers: { 'Authorization': `Bearer ${token}` }
    });
    
    const result = await response.json();
    
    if (!response.ok || !result.success) {
        return errorResponse(`Marketo API Error: ${JSON.stringify(result.errors)}`, 502);
    }
    
    // Filter for fields that are usable in the REST API
    const usableFields = result.result.filter(field => field.rest);
    
    return jsonResponse({
        status: 'success',
        fields: usableFields
    });
}

/**
 * 4. Handles Program Name Search: Calls /rest/v1/programs.json.
 * /api/programs (GET)
 */
async function handleProgramSearch(request, env) {
    const url = new URL(request.url);
    const searchQuery = url.searchParams.get('search');
    if (!searchQuery) {
        return errorResponse('Missing "search" query parameter.', 400);
    }
    
    const token = await getAccessToken(env);
    // Search for programs by name
    const searchUrl = `${env.MARKETO_ENDPOINT}/rest/v1/programs.json?filterType=programName&filterValues=${encodeURIComponent(searchQuery)}`;
    
    const response = await fetch(searchUrl, {
        method: 'GET',
        headers: { 'Authorization': `Bearer ${token}` }
    });
    
    const result = await response.json();
    
    if (!response.ok || !result.success) {
        return errorResponse(`Marketo API Error: ${JSON.stringify(result.errors)}`, 502);
    }
    
    return jsonResponse({
        status: 'success',
        programs: result.result
    });
}

/**
 * 5. Handles Gemini Data Parsing: Calls Google AI API for structured output.
 * /api/parse-leads (POST)
 */
async function handleGeminiParse(request, env) {
    const body = await request.json();
    const { text, jsonSchema } = body;
    if (!text || !jsonSchema) {
        return errorResponse('Missing "text" (the unstructured data) or "jsonSchema" (the output format).', 400);
    }

    const apiKey = ""; // API key is managed by the runtime
    const apiUrl = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key=${apiKey}`;

    const systemPrompt = `You are an expert data parsing assistant. A user will provide messy, unstructured text containing lead data. Your ONLY job is to extract this data and return it in the exact JSON format requested. Do not add any commentary. Only return the JSON.`;

    const payload = {
        contents: [{
            parts: [{ text: `Parse the following text: ${text}` }]
        }],
        systemInstruction: {
            parts: [{ text: systemPrompt }]
        },
        generationConfig: {
            responseMimeType: "application/json",
            responseSchema: jsonSchema
        }
    };

    const response = await fetch(apiUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });

    if (!response.ok) {
        return errorResponse(`Gemini API Error: ${await response.text()}`, 502);
    }

    const result = await response.json();
    const geminiText = result.candidates?.[0]?.content?.parts?.[0]?.text;

    if (!geminiText) {
        return errorResponse('Gemini returned an empty response.', 500);
    }

    try {
        const parsedJson = JSON.parse(geminiText);
        return jsonResponse({ status: 'success', data: parsedJson });
    } catch (e) {
        return errorResponse(`Failed to parse Gemini JSON response: ${e.message}`, 500);
    }
}

/**
 * 6. Handles Gemini Job Title Description: Calls Google AI API for text.
 * /api/describe-title (POST)
 */
async function handleGeminiDescribe(request, env) {
    const body = await request.json();
    const { title } = body;
    if (!title) {
        return errorResponse('Missing "title".', 400);
    }

    const apiKey = ""; // API key is managed by the runtime
    const apiUrl = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key=${apiKey}`;

    const systemPrompt = `You are a professional recruiting assistant. A user will provide a job title. Your job is to return a single, professional, one-sentence description of that role's primary responsibility. Do not add any commentary. Only return the single sentence.`;
    const userQuery = `Job Title: ${title}`;

    const payload = {
        contents: [{ parts: [{ text: userQuery }] }],
        systemInstruction: { parts: [{ text: systemPrompt }] },
    };

    const response = await fetch(apiUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });

    if (!response.ok) {
        return errorResponse(`Gemini API Error: ${await response.text()}`, 502);
    }
    
    const result = await response.json();
    const description = result.candidates?.[0]?.content?.parts?.[0]?.text;

    if (!description) {
        return errorResponse('Gemini returned an empty description.', 500);
    }

    return jsonResponse({ status: 'success', description: description.trim() });
}


// --- Marketo Bulk API Helper Functions ---

/**
 * Step 1 & 2 of Bulk Import: Create Job and Upload Data
 * @param {object} env
 * @param {string} token
 * @param {string} csvContent
 * @param {string} lookupField
 * @returns {Promise<{batchId: string, status: string}>}
 */
async function createImportJobAndUploadData(env, token, csvContent, lookupField) {
    // Step 1: Create the import job
    const createUrl = `${env.MARKETO_ENDPOINT}/bulk/v1/leads.json?format=csv&lookupField=${lookupField}`;
    
    const createResponse = await fetch(createUrl, {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${token}`,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({}) // Empty body to just create the job
    });
    
    const createResult = await createResponse.json();
    
    if (!createResponse.ok || !createResult.success || !createResult.result[0].batchId) {
        throw new Error(`Marketo failed to create bulk job: ${JSON.stringify(createResult.errors)}`);
    }
    
    const batchId = createResult.result[0].batchId;

    // Step 2: Upload the CSV data to the job
    const uploadUrl = `${env.MARKETO_ENDPOINT}/bulk/v1/leads.json?batchId=${batchId}`;
    
    // We must use multipart/form-data for the file upload
    const formData = new FormData();
    formData.append('file', new Blob([csvContent], { type: 'text/csv' }), 'import.csv');

    const uploadResponse = await fetch(uploadUrl, {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${token}`,
            // Content-Type is set automatically by fetch when using FormData
        },
        body: formData
    });

    const uploadResult = await uploadResponse.json();
    
    if (!uploadResponse.ok || !uploadResult.success) {
        throw new Error(`Marketo failed to upload CSV data: ${JSON.stringify(uploadResult.errors)}`);
    }

    return {
        batchId: batchId,
        status: uploadResult.result[0].status
    };
}

/**
 * Step 3 of Bulk Import: Poll job status until completion.
 * @param {object} env
 * @param {string} token
 * @param {string} batchId
 * @returns {Promise<object>} The final status object from Marketo.
 */
async function pollJobStatus(env, token, batchId) {
    const pollUrl = `${env.MARKETO_ENDPOINT}/bulk/v1/leads/batch/${batchId}.json`;
    let attempt = 0;
    
    while (attempt < 10) { // Max 10 attempts
        await pollDelay(attempt); // Exponential backoff
        
        const response = await fetch(pollUrl, {
            method: 'GET',
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        const result = await response.json();
        
        if (!response.ok || !result.success) {
            throw new Error(`Marketo job polling failed: ${JSON.stringify(result.errors)}`);
        }
        
        const status = result.result[0].status;
        
        if (status === 'Completed') {
            return result.result[0]; // Success!
        }
        
        if (status === 'Failed') {
            throw new Error(`Marketo Job Failed. Batch ID: ${batchId}. Errors: ${JSON.stringify(result.result[0].errors)}`);
        }
        
        // If status is 'Queued' or 'Processing', continue polling
        attempt++;
    }
    
    throw new Error(`Marketo job timed out after ${attempt} attempts. Batch ID: ${batchId}`);
}

