// Cloudflare Worker Script for Marketo Bulk Import Handling
// This worker serves as a secure proxy, hiding Marketo credentials from the frontend client.

// CORS headers for allowing the frontend (Cloudflare Pages) to communicate with this worker
const CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*', // IMPORTANT: Adjust this in production to your Pages domain
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json',
};

/**
 * Handles the OPTIONS request (CORS preflight check).
 * @returns {Response}
 */
function handleOptions() {
    return new Response(null, {
        status: 204, // No Content
        headers: CORS_HEADERS,
    });
}

/**
 * Retrieves a Marketo access token using Client ID and Secret from environment variables.
 * @param {object} env - Cloudflare Worker environment variables.
 * @returns {Promise<string>} The access token string.
 */
async function getAccessToken(env) {
    const url = `${env.MARKETO_ENDPOINT}/identity/oauth/token?grant_type=client_credentials&client_id=${env.MARKETO_CLIENT_ID}&client_secret=${env.MARKETO_CLIENT_SECRET}`;

    const response = await fetch(url, { method: 'GET' });
    const json = await response.json();

    if (!response.ok || !json.access_token) {
        // Log the full error to the worker console for debugging
        console.error('Marketo Auth Error:', json);
        throw new Error('Failed to retrieve Marketo access token. Check Client ID/Secret.');
    }

    return json.access_token;
}

/**
 * Handles the GET request to fetch Marketo field metadata.
 * @param {object} env - Cloudflare Worker environment variables.
 * @returns {Promise<Response>}
 */
async function handleGetFields(env) {
    try {
        const accessToken = await getAccessToken(env);
        // Endpoint to describe lead fields
        const url = `${env.MARKETO_ENDPOINT}/rest/v1/leads/describe.json?access_token=${accessToken}`;

        const response = await fetch(url, { method: 'GET' });
        const json = await response.json();

        if (!response.ok || json.success !== true) {
            console.error('Marketo Describe Error:', json);
            throw new Error('Failed to retrieve Marketo field description.');
        }

        // Return only the necessary field metadata array
        return new Response(JSON.stringify({
            status: 'success',
            fields: json.result
        }), {
            status: 200,
            headers: CORS_HEADERS,
        });

    } catch (error) {
        return new Response(JSON.stringify({ status: 'error', message: error.message }), {
            status: 500,
            headers: CORS_HEADERS,
        });
    }
}

/**
 * Handles the GET request to search Marketo programs for autocomplete.
 * @param {object} env - Cloudflare Worker environment variables.
 * @param {string} searchString - The query string from the user.
 * @returns {Promise<Response>}
 */
async function handleGetPrograms(env, searchString) {
    if (!searchString) {
        return new Response(JSON.stringify({ status: 'success', programs: [] }), { status: 200, headers: CORS_HEADERS });
    }

    try {
        const accessToken = await getAccessToken(env);
        
        // Marketo's program search API endpoint
        // Filter by program type (2 == Program) and use the search parameter
        const url = `${env.MARKETO_ENDPOINT}/rest/asset/v1/programs.json?access_token=${accessToken}&type=2&name=${encodeURIComponent(searchString)}`;

        const response = await fetch(url, { method: 'GET' });
        const json = await response.json();

        if (!response.ok || json.success !== true) {
            console.error('Marketo Program Search Error:', json);
            // Don't throw a full error, just return an empty list for a failed search
            return new Response(JSON.stringify({ status: 'error', programs: [] }), {
                status: 200,
                headers: CORS_HEADERS,
            });
        }
        
        // Map the results to a simple list of names for the frontend
        const programNames = json.result
            .filter(program => program.name && program.status === 'Approved') // Filter for approved programs
            .map(program => program.name)
            .slice(0, 10); // Limit results for UI performance

        return new Response(JSON.stringify({
            status: 'success',
            programs: programNames
        }), {
            status: 200,
            headers: CORS_HEADERS,
        });

    } catch (error) {
        return new Response(JSON.stringify({ status: 'error', message: error.message }), {
            status: 500,
            headers: CORS_HEADERS,
        });
    }
}


/**
 * 2a. Creates the import job and 2b. Uploads the data file.
 * @param {object} env - Environment variables.
 * @param {string} accessToken - Marketo OAuth token.
 * @param {string} csvContent - CSV data string (already mapped).
 * @param {string} lookupField - The deduplication field (hardcoded as 'email').
 * @returns {Promise<string>} The batch ID of the created job.
 */
async function createImportJobAndUploadData(env, accessToken, csvContent, lookupField) {
    // 2a. Create the Job
    let createJobUrl = `${env.MARKETO_ENDPOINT}/rest/bulk/v1/leads/create.json?access_token=${accessToken}&format=csv&lookupField=${lookupField}`;

    let response = await fetch(createJobUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
    });
    let json = await response.json();

    if (!response.ok || json.success !== true || !json.result || !json.result[0] || !json.result[0].batchId) {
        console.error('Create Job Error:', json);
        throw new Error('Marketo failed to create bulk import job.');
    }

    const batchId = json.result[0].batchId;

    // 2b. Upload the Data File
    const uploadUrl = `${env.MARKETO_ENDPOINT}/rest/bulk/v1/leads/${batchId}/file?access_token=${accessToken}`;
    
    // Create a Blob from the CSV string, which fetch can handle
    const csvBlob = new Blob([csvContent], { type: 'text/csv' });

    response = await fetch(uploadUrl, {
        method: 'POST',
        headers: { 
            // Marketo API requires Content-Type: text/csv for the file upload
            'Content-Type': 'text/csv',
            'Accept': 'application/json',
        },
        body: csvBlob,
    });

    // Marketo doesn't return a body on success (200), so we only check the status
    if (response.status !== 200) {
        const errorText = await response.text();
        console.error('Upload Data Error:', errorText);
        throw new Error(`Marketo failed to upload data for batch ${batchId}. Status: ${response.status}`);
    }

    // Success: return the batch ID
    return batchId;
}

/**
 * Polls the job status until completion or failure using exponential backoff.
 * @param {object} env - Environment variables.
 * @param {string} accessToken - Marketo OAuth token.
 * @param {string} batchId - The batch ID to check.
 * @returns {Promise<object>} The final result object from Marketo status endpoint.
 */
async function pollJobStatus(env, accessToken, batchId) {
    const maxAttempts = 10;
    let attempt = 0;
    
    while (attempt < maxAttempts) {
        attempt++;
        const pollUrl = `${env.MARKETO_ENDPOINT}/rest/bulk/v1/leads/batch/${batchId}/status?access_token=${accessToken}`;
        
        const response = await fetch(pollUrl, { method: 'GET' });
        const json = await response.json();

        if (!response.ok || json.success !== true || !json.result || json.result.length === 0) {
            console.error('Poll Status Error:', json);
            throw new Error(`Polling failed on attempt ${attempt}.`);
        }
        
        const job = json.result[0];
        const status = job.status;

        if (status === 'Completed' || status === 'Failed' || status === 'Cancelled') {
            return job; // Return final result
        }

        // Wait with exponential backoff: 2s, 4s, 8s, 16s, ...
        const delay = Math.min(2000 * Math.pow(2, attempt - 1), 60000); // Max delay 60s
        await new Promise(resolve => setTimeout(resolve, delay));
    }
    
    throw new Error('Marketo job polling timed out.');
}

/**
 * Handles the POST request to start the Marketo bulk import process.
 * This function orchestrates the three steps: 1) Auth, 2) Create Job + Upload Data, 3) Poll Status.
 * @param {Request} request - The incoming request from the frontend.
 * @param {object} env - Cloudflare Worker environment variables.
 * @returns {Promise<Response>}
 */
async function handleImport(request, env) {
    let requestBody;
    try {
        requestBody = await request.json();
    } catch (e) {
        return new Response(JSON.stringify({ status: 'error', message: 'Invalid JSON body' }), { status: 400, headers: CORS_HEADERS });
    }

    const { csvContent, lookupField, programName, fieldMapping } = requestBody;

    if (!csvContent || !lookupField || !programName) {
        return new Response(JSON.stringify({ status: 'error', message: 'Missing required fields (csvContent, lookupField, or programName).' }), { status: 400, headers: CORS_HEADERS });
    }

    try {
        // Step 1: Authentication
        const accessToken = await getAccessToken(env);

        // Step 2: Create Job and Upload Data
        const batchId = await createImportJobAndUploadData(env, accessToken, csvContent, lookupField);

        // Step 3: Poll Status (This is typically done by a separate job or a short loop)
        const jobResult = await pollJobStatus(env, accessToken, batchId);

        // Final success response to the client
        return new Response(JSON.stringify({
            status: jobResult.status,
            jobId: batchId,
            programName: programName,
            successRows: jobResult.numOfRowsProcessed,
            errorRows: jobResult.numOfRowsWithErrors,
            message: `Marketo Bulk Import Job ${jobResult.status}.`
        }), {
            status: 200,
            headers: CORS_HEADERS,
        });

    } catch (error) {
        console.error('Import process failed:', error.message);
        return new Response(JSON.stringify({ status: 'error', message: `Import failed: ${error.message}` }), {
            status: 500,
            headers: CORS_HEADERS,
        });
    }
}


// The main Worker listener for incoming requests
addEventListener('fetch', event => {
    event.respondWith(handleRequest(event.request, event.env));
});

/**
 * Primary request handler for routing.
 * @param {Request} request - The incoming request.
 * @param {object} env - Environment variables.
 * @returns {Promise<Response>}
 */
async function handleRequest(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const searchParams = url.searchParams;

    // Handle CORS preflight request
    if (request.method === 'OPTIONS') {
        return handleOptions();
    }

    // Route GET requests for Marketo fields metadata
    if (path === '/api/fields' && request.method === 'GET') {
        return handleGetFields(env);
    }
    
    // Route GET requests for program autocomplete search
    if (path === '/api/programs' && request.method === 'GET') {
        const searchString = searchParams.get('search') || '';
        return handleGetPrograms(env, searchString);
    }
    
    // Route POST requests for bulk import
    if (path === '/api/import' && request.method === 'POST') {
        return handleImport(request, env);
    }

    // Default response for unhandled paths
    return new Response(JSON.stringify({ status: 'error', message: 'Not Found' }), {
        status: 404,
        headers: CORS_HEADERS,
    });
}
