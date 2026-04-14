/**
 * ADLS Gen2 REST API Client
 * Uses Azure Identity for authentication and REST API for file operations
 */

const { AzureCliCredential } = require('@azure/identity');

class ADLSRestClient {
    constructor(storageAccountName) {
        this.storageAccountName = storageAccountName;
        this.baseUrl = `https://${storageAccountName}.dfs.core.windows.net`;
        this.credential = new AzureCliCredential();
        this.scope = 'https://storage.azure.com/.default';
    }

    /**
     * Get access token for Azure Storage
     */
    async getAccessToken() {
        try {
            const tokenResponse = await this.credential.getToken(this.scope);
            return tokenResponse.token;
        } catch (error) {
            console.error('Failed to get access token:', error);
            throw error;
        }
    }

    /**
     * List paths (files and directories) in a container
     * @param {string} containerName - The container name
     * @param {string} directory - Directory path (optional)
     * @param {boolean} recursive - Whether to list recursively
     */
    async listPaths(containerName, directory = '', recursive = false) {
        const token = await this.getAccessToken();
        
        let url = `${this.baseUrl}/${containerName}?resource=filesystem&recursive=${recursive}`;
        if (directory) {
            url += `&directory=${encodeURIComponent(directory)}`;
        }

        const response = await fetch(url, {
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${token}`,
                'x-ms-version': '2020-02-10'
            }
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Failed to list paths: ${response.status} ${response.statusText}\n${errorText}`);
        }

        const data = await response.json();
        return data.paths || [];
    }

    /**
     * Read file content from ADLS Gen2 / Blob.
     * Tries the DFS endpoint first; if the account has SoftDelete/BlobStorageEvents
     * enabled (HTTP 409 EndpointUnsupportedAccountFeatures) it automatically falls
     * back to the Blob endpoint.
     * @param {string} containerName - The container name
     * @param {string} filePath - Path to the file
     */
    async readFile(containerName, filePath) {
        const token = await this.getAccessToken();
        const encodedPath = filePath.split('/').map(segment => encodeURIComponent(segment)).join('/');
        const headers = { 'Authorization': `Bearer ${token}`, 'x-ms-version': '2020-02-10' };

        // Try DFS endpoint first
        const dfsUrl = `${this.baseUrl}/${containerName}/${encodedPath}`;
        const dfsResp = await fetch(dfsUrl, { method: 'GET', headers });
        if (dfsResp.ok) return await dfsResp.text();

        const errorText = await dfsResp.text();
        // Fall back to Blob endpoint on 409 EndpointUnsupportedAccountFeatures
        if (dfsResp.status === 409 && errorText.includes('EndpointUnsupportedAccountFeatures')) {
            const blobUrl = `https://${this.storageAccountName}.blob.core.windows.net/${containerName}/${encodedPath}`;
            const blobResp = await fetch(blobUrl, { method: 'GET', headers });
            if (blobResp.ok) return await blobResp.text();
            const blobErr = await blobResp.text();
            throw new Error(`Failed to read file (blob fallback): ${blobResp.status} ${blobResp.statusText}\n${blobErr}`);
        }
        throw new Error(`Failed to read file: ${dfsResp.status} ${dfsResp.statusText}\n${errorText}`);
    }

    /**
     * Get file properties
     * @param {string} containerName - The container name
     * @param {string} filePath - Path to the file
     */
    async getFileProperties(containerName, filePath) {
        const token = await this.getAccessToken();
        
        const url = `${this.baseUrl}/${containerName}/${encodeURIComponent(filePath)}`;

        const response = await fetch(url, {
            method: 'HEAD',
            headers: {
                'Authorization': `Bearer ${token}`,
                'x-ms-version': '2020-02-10'
            }
        });

        if (!response.ok) {
            throw new Error(`Failed to get file properties: ${response.status} ${response.statusText}`);
        }

        return {
            lastModified: response.headers.get('last-modified'),
            contentLength: response.headers.get('content-length'),
            contentType: response.headers.get('content-type'),
            etag: response.headers.get('etag')
        };
    }

    /**
     * Get all activity_runs.json files from pipeline-runs folder
     * @param {string} containerName - The container name
     */
    async getPipelineRunFiles(containerName, pipelineRunsFolder = 'pipeline-runs') {
        try {
            // List all directories in pipeline-runs folder
            const paths = await this.listPaths(containerName, pipelineRunsFolder, false);
            
            const results = [];
            
            // For each directory, try to read activity_runs.json
            for (const path of paths) {
                if (path.isDirectory) {
                    const dirName = path.name.split('/').pop();
                    const activityRunsPath = `${pipelineRunsFolder}/${dirName}/activity_runs.json`;
                    
                    try {
                        const content = await this.readFile(containerName, activityRunsPath);
                        const parsed = JSON.parse(content);
                        const activities = parsed.value || parsed; // Handle both {value: [...]} and [...] formats
                        results.push({
                            folder: dirName,
                            path: activityRunsPath,
                            content: parsed,
                            activities: activities
                        });
                        console.log(`✅ Successfully read: ${activityRunsPath}`);
                    } catch (error) {
                        console.error(`❌ Failed to read ${activityRunsPath}:`, error.message);
                    }
                }
            }
            
            return results;
        } catch (error) {
            console.error('Error getting pipeline run files:', error);
            throw error;
        }
    }

    /**
     * Write (create or overwrite) a file in ADLS Gen2 using the 3-step DFS API:
     * 1. Create (or overwrite) the path
     * 2. Append the data
     * 3. Flush
     * @param {string} containerName - The container name
     * @param {string} filePath - Destination path inside the container
     * @param {string} content - UTF-8 text content to write
     */
    async writeFile(containerName, filePath, content) {
        const token = await this.getAccessToken();
        const encodedPath = filePath.split('/').map(segment => encodeURIComponent(segment)).join('/');
        const dfsBase = `${this.baseUrl}/${containerName}/${encodedPath}`;
        const data = Buffer.from(content, 'utf-8');
        const contentLength = data.length;
        const authHeaders = { 'Authorization': `Bearer ${token}`, 'x-ms-version': '2020-02-10' };

        // Step 1: Create (overwrite if exists)
        const createResponse = await fetch(`${dfsBase}?resource=file&overwrite=true`, {
            method: 'PUT',
            headers: { ...authHeaders, 'Content-Length': '0' }
        });
        // DFS not supported on this account — fall back to Blob single-PUT
        if (!createResponse.ok) {
            const createErr = await createResponse.text();
            if (createResponse.status === 409 && createErr.includes('EndpointUnsupportedAccountFeatures')) {
                const blobUrl = `https://${this.storageAccountName}.blob.core.windows.net/${containerName}/${encodedPath}`;
                const blobResp = await fetch(blobUrl, {
                    method: 'PUT',
                    headers: { ...authHeaders, 'x-ms-blob-type': 'BlockBlob', 'Content-Length': String(contentLength), 'Content-Type': 'application/octet-stream' },
                    body: data
                });
                if (!blobResp.ok) {
                    const blobErr = await blobResp.text();
                    throw new Error(`ADLS write failed (blob fallback): ${blobResp.status} ${blobResp.statusText}\n${blobErr}`);
                }
                return;
            }
            throw new Error(`ADLS create failed: ${createResponse.status} ${createResponse.statusText}\n${createErr}`);
        }

        // Step 2: Append data
        const appendResponse = await fetch(`${dfsBase}?action=append&position=0`, {
            method: 'PATCH',
            headers: { ...authHeaders, 'Content-Length': String(contentLength), 'Content-Type': 'application/octet-stream' },
            body: data
        });
        if (!appendResponse.ok) {
            const err = await appendResponse.text();
            throw new Error(`ADLS append failed: ${appendResponse.status} ${appendResponse.statusText}\n${err}`);
        }

        // Step 3: Flush
        const flushResponse = await fetch(`${dfsBase}?action=flush&position=${contentLength}`, {
            method: 'PATCH',
            headers: { ...authHeaders, 'Content-Length': '0' }
        });
        if (!flushResponse.ok) {
            const err = await flushResponse.text();
            throw new Error(`ADLS flush failed: ${flushResponse.status} ${flushResponse.statusText}\n${err}`);
        }
    }

    /**
     * Delete a file in ADLS Gen2.
     * @param {string} containerName - The container name
     * @param {string} filePath - Path to the file to delete
     */
    async deleteFile(containerName, filePath) {
        const token = await this.getAccessToken();
        const encodedPath = filePath.split('/').map(segment => encodeURIComponent(segment)).join('/');
        const url = `${this.baseUrl}/${containerName}/${encodedPath}`;

        const response = await fetch(url, {
            method: 'DELETE',
            headers: {
                'Authorization': `Bearer ${token}`,
                'x-ms-version': '2020-02-10'
            }
        });

        if (!response.ok) {
            const err = await response.text();
            throw new Error(`ADLS delete failed: ${response.status} ${response.statusText}\n${err}`);
        }
    }

    /**
     * Get notebook snapshot from pipeline run
     * @param {string} containerName - The container name
     * @param {string} runFolder - Pipeline run folder name
     * @param {string} activityRunId - Activity run ID
     */
    async getNotebookSnapshot(containerName, runFolder, activityRunId) {
        try {
            // First, check if notebooks folder exists
            const notebooksFolder = `pipeline-runs/${runFolder}/notebooks`;
            
            const notebookFiles = await this.listPaths(containerName, notebooksFolder, false);
            console.log(`Found ${notebookFiles.length} files in notebooks folder:`);
            notebookFiles.forEach(file => {
                console.log(`  - ${file.name}`);
            });
            
            if (notebookFiles.length === 0) {
                throw new Error(`Notebooks folder is empty. Path: ${notebooksFolder}`);
            }
            
            // Find the file that contains the activityRunId
            // The filename pattern is: {NotebookName}_{activityRunId}.json
            const matchingFile = notebookFiles.find(file => {
                const fileName = file.name.split('/').pop();
                return fileName.includes(activityRunId) && fileName.endsWith('.json');
            });
            
            if (!matchingFile) {
                throw new Error(`Notebook snapshot file not found for activity run ID: ${activityRunId}. Available files: ${notebookFiles.map(f => f.name.split('/').pop()).join(', ')}`);
            }
            
            console.log(`Reading notebook file: ${matchingFile.name}`);
            const content = await this.readFile(containerName, matchingFile.name);
            return JSON.parse(content);
        } catch (error) {
            console.error(`Failed to read notebook snapshot: ${error.message}`);
            throw error;
        }
    }
}

/**
 * Test function to verify ADLS access
 */
async function testADLSAccess() {
    const storageAccountName = 'testadlsjervis123';
    const containerName = 'confirmed';
    
    console.log('🔐 Initializing ADLS REST Client...');
    const client = new ADLSRestClient(storageAccountName);
    
    try {
        console.log('\n📂 Listing directories in pipeline-runs folder...');
        const paths = await client.listPaths(containerName, 'pipeline-runs', false);
        console.log(`Found ${paths.length} items:`);
        paths.forEach(path => {
            console.log(`  ${path.isDirectory ? '📁' : '📄'} ${path.name}`);
        });
        
        console.log('\n📥 Retrieving activity_runs.json files...');
        const pipelineRuns = await client.getPipelineRunFiles(containerName);
        
        console.log(`\n✅ Successfully retrieved ${pipelineRuns.length} activity_runs.json files:`);
        pipelineRuns.forEach(run => {
            console.log(`\n📊 Folder: ${run.folder}`);
            console.log(`   Path: ${run.path}`);
            console.log(`   Activities: ${Array.isArray(run.activities) ? run.activities.length : 'N/A'}`);
            if (Array.isArray(run.activities)) {
                run.activities.forEach((activity, idx) => {
                    console.log(`     ${idx + 1}. ${activity.activityName || 'Unknown'} - ${activity.activityType || 'Unknown'} - Duration: ${activity.durationInMs}ms`);
                });
            }
        });
        
        return pipelineRuns;
    } catch (error) {
        console.error('\n❌ Error:', error.message);
        throw error;
    }
}

// Export the client and test function
module.exports = {
    ADLSRestClient,
    testADLSAccess
};

// If running directly (not imported), run the test
if (require.main === module) {
    testADLSAccess()
        .then(() => {
            console.log('\n✅ Test completed successfully!');
            process.exit(0);
        })
        .catch((error) => {
            console.error('\n❌ Test failed:', error);
            process.exit(1);
        });
}
