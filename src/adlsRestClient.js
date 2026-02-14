/**
 * ADLS Gen2 REST API Client
 * Uses Azure Identity for authentication and REST API for file operations
 */

const { DefaultAzureCredential } = require('@azure/identity');

class ADLSRestClient {
    constructor(storageAccountName) {
        this.storageAccountName = storageAccountName;
        this.baseUrl = `https://${storageAccountName}.dfs.core.windows.net`;
        this.credential = new DefaultAzureCredential();
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
     * Read file content from ADLS
     * @param {string} containerName - The container name
     * @param {string} filePath - Path to the file
     */
    async readFile(containerName, filePath) {
        const token = await this.getAccessToken();
        
        // Encode each path segment separately to preserve slashes
        const encodedPath = filePath.split('/').map(segment => encodeURIComponent(segment)).join('/');
        const url = `${this.baseUrl}/${containerName}/${encodedPath}`;

        const response = await fetch(url, {
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${token}`,
                'x-ms-version': '2020-02-10'
            }
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Failed to read file: ${response.status} ${response.statusText}\n${errorText}`);
        }

        return await response.text();
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
