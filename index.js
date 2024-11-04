const express = require('express');
const multer = require('multer');
const { BlobServiceClient } = require('@azure/storage-blob');
const cors = require('cors');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

// Initialize Express app
const app = express();
const PORT = process.env.PORT | 3000;

// Set up Azure Blob Service
const AZURE_STORAGE_CONNECTION_STRING = 'DefaultEndpointsProtocol=https;AccountName=contentservstorageacc;AccountKey=xA1tP7sDdIXu/QwcW6yYeFLUQ1bfjQKOXA6Sk5MBVQ+3EsbEDcRe4y7jOrJZ4WEuPrKQkhjDkUnH+AStICbLOQ==;EndpointSuffix=core.windows.net';
const containerName = 'pdfprocess';

// Initialize BlobServiceClient
const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);

app.use(cors());
app.use(express.json());

const upload = multer({ storage: multer.memoryStorage() });

function getMimeType(filename) {
    const ext = path.extname(filename).toLowerCase();
    if (ext === '.pdf') return 'application/pdf';
    if (ext === '.png') return 'image/png';
    if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
    return 'application/octet-stream'; // Default type
}

async function moveBlobToSucceededFolder(blobName) {
    const containerClient = blobServiceClient.getContainerClient(containerName);
    const sourceBlob = containerClient.getBlobClient(blobName);
    const destinationBlob = containerClient.getBlobClient(`succeeded_files/${blobName}`);

    console.log(`Attempting to move blob from ${blobName} to succeeded_files/${blobName}`);

    try {
        // Check if the source blob exists
        const sourceExists = await sourceBlob.exists();
        if (!sourceExists) {
            console.log(`Source blob ${blobName} does not exist`);
            return;
        }

        // Start the copy operation
        const copyPoller = await destinationBlob.beginCopyFromURL(sourceBlob.url);
        console.log(`Copy operation started for ${blobName}`);

        // Wait for the copy operation to complete
        const copyResult = await copyPoller.pollUntilDone();
        console.log(`Copy operation completed with status: ${copyResult.copyStatus}`);

        // If copy was successful, delete the source blob
        if (copyResult.copyStatus === "success") {
            console.log(`Deleting source blob ${blobName}`);
            await sourceBlob.delete();
            console.log(`Source blob ${blobName} deleted successfully`);
        } else {
            console.log(`Copy operation did not succeed for ${blobName}. Status: ${copyResult.copyStatus}`);
        }
    } catch (error) {
        console.error(`Error moving blob ${blobName}:`, error);
        throw error;
    }
}

app.post('/files/upload', upload.array('files'), async (req, res) => {
    const files = req.files;
    let tags;
    
    try {
        tags = JSON.parse(req.body.tags);
    } catch (error) {
        tags = [req.body.tags];
    }
    
    try {
        for (let i = 0; i < files.length; i++) {
            const file = files[i];
            const fileId = uuidv4();
            const blobName = `${fileId}-${file.originalname}`;
            const containerClient = blobServiceClient.getContainerClient(containerName);
            const blockBlobClient = containerClient.getBlockBlobClient(blobName);

            const contentType = getMimeType(file.originalname);

            const metadata = {
                tags: Array.isArray(tags[i]) ? tags[i].join(',') : tags[i]
            };

            await blockBlobClient.uploadData(file.buffer, {
                blobHTTPHeaders: { blobContentType: contentType },
                metadata: metadata
            });
        }

        res.status(200).send({ message: 'Files uploaded to Azure Blob Storage successfully!' });
    } catch (error) {
        console.error('Error uploading files to Azure Blob Storage:', error);
        res.status(500).send({ message: 'Error uploading files to Azure Blob Storage' });
    }
});

app.get('/files', async (req, res) => {
    try {
        const containerClient = blobServiceClient.getContainerClient(containerName);
        const blobs = containerClient.listBlobsFlat();

        const files = [];
        for await (const blob of blobs) {
            const blobClient = containerClient.getBlobClient(blob.name);
            const properties = await blobClient.getProperties();
            
            files.push({
                name: blob.name.split('-').slice(1).join('-'), // Remove UUID prefix
                blobName: blob.name,
                blobUrl: blobClient.url,
                contentType: properties.contentType,
                tags: properties.metadata.tags ? properties.metadata.tags.split(',') : [],
                status: blob.name.startsWith('succeeded_files/') ? 'Processed' : 'Pending'
            });
        }

        res.status(200).json(files);
        
    } catch (error) {
        console.error('Error retrieving files from Azure Blob Storage:', error);
        res.status(500).send({ message: 'Error retrieving files from Azure Blob Storage' });
    }
});

app.delete('/files/:name', async (req, res) => {
    const fileName = req.params.name;

    try {
        const containerClient = blobServiceClient.getContainerClient(containerName);
        const blobs = containerClient.listBlobsFlat();

        for await (const blob of blobs) {
            if (blob.name.endsWith(fileName)) {
                await containerClient.deleteBlob(blob.name);
                res.status(200).send({ message: 'File deleted successfully from Azure Blob Storage!' });
                return;
            }
        }

        res.status(404).send({ message: 'File not found' });
    } catch (error) {
        console.error('Error deleting file from Azure Blob Storage:', error);
        res.status(500).send({ message: 'Error deleting file from Azure Blob Storage' });
    }
});

app.get('/files/:name/details', async (req, res) => {
    const fileName = req.params.name;

    try {
        const containerClient = blobServiceClient.getContainerClient(containerName);
        const blobs = containerClient.listBlobsFlat();

        for await (const blob of blobs) {
            if (blob.name.endsWith(fileName)) {
                const blobClient = containerClient.getBlobClient(blob.name);
                const properties = await blobClient.getProperties();

                res.status(200).json({
                    name: fileName,
                    contentType: properties.contentType,
                    tags: properties.metadata.tags ? properties.metadata.tags.split(',') : []
                });
                return;
            }
        }

        res.status(404).send({ message: 'File not found' });
    } catch (error) {
        console.error('Error getting file details from Azure Blob Storage:', error);
        res.status(500).send({ message: 'Error getting file details from Azure Blob Storage' });
    }
});

let latestNotification = null;

app.post('/notify', async (req, res) => {
    const { message, file_name } = req.body;
    console.log(`Received notification: ${message}: ${file_name}`);
    
    latestNotification = { message, file_name };
    
    // Check if the message indicates successful processing
    if (message.toLowerCase().includes('processed successfully')) {
        try {
            const containerClient = blobServiceClient.getContainerClient(containerName);
            const blobs = containerClient.listBlobsFlat();

            // Extract the UUID from the notification file_name
            const notificationUUID = file_name.split('-')[0];
            
            let blobMoved = false;
            for await (const blob of blobs) {
                // Check if the blob name starts with the UUID from the notification
                if (blob.name.startsWith(notificationUUID)) {
                    await moveBlobToSucceededFolder(blob.name);
                    console.log(`Moved ${blob.name} to succeeded_files folder`);
                    blobMoved = true;
                    break;
                }
            }

            if (!blobMoved) {
                console.log(`No matching blob found for UUID: ${notificationUUID}`);
            }
        } catch (error) {
            console.error('Error moving file to succeeded_files folder:', error);
        }
    }
    
    res.status(200).send('Notification received');
});

app.get('/latest-notification', (req, res) => {
    res.json(latestNotification || {});
    latestNotification = null; // Clear the notification after sending
});

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
