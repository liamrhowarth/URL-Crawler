const fs = require('fs');
const path = require('path');
const csvParser = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

// File paths
const CRAWLED_URLS_PATH = path.join(__dirname, '../output/crawled_urls_JSAI_Live.csv');
const PRODUCT_EXPORT_PATH = path.join(__dirname, '../output/jsai_product_export_shopify.csv');
const OUTPUT_PATH = path.join(__dirname, '../output/url-matchers.csv');

// Function to load a CSV file into an array of objects
async function loadCsv(filePath) {
  const data = [];
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on('data', (row) => data.push(row))
      .on('end', () => resolve(data))
      .on('error', (err) => reject(err));
  });
}

// Function to extract the final part of a URL path
function extractFinalPath(url) {
  try {
    const urlObj = new URL(url);
    const pathParts = urlObj.pathname.split('/').filter(Boolean); // Split and remove empty segments
    return pathParts[pathParts.length - 1]; // Return the last part
  } catch (err) {
    console.error(`Invalid URL: ${url}`);
    return null;
  }
}

// Main matching function
async function matchUrls() {
  try {
    // Load CSVs
    const crawledUrls = await loadCsv(CRAWLED_URLS_PATH);
    const productExport = await loadCsv(PRODUCT_EXPORT_PATH);

    // Extract Handles into a Set for quick lookup
    const handles = new Set(productExport.map((row) => row.Handle));

    // Process crawled URLs and match with Handles
    const results = crawledUrls.map((row) => {
      const originalUrl = row.URL;
      const finalPath = extractFinalPath(originalUrl);
      const matchedHandle = handles.has(finalPath) ? `/products/${finalPath}` : null;
      return { originalUrl, matchedHandle };
    });

    // Write results to output CSV
    const csvWriter = createCsvWriter({
      path: OUTPUT_PATH,
      header: [
        { id: 'originalUrl', title: 'Original URL' },
        { id: 'matchedHandle', title: 'Matched Handle' },
      ],
    });

    await csvWriter.writeRecords(results);
    console.log(`Output saved to ${OUTPUT_PATH}`);
  } catch (err) {
    console.error(`Error during matching process: ${err.message}`);
  }
}

// Run the script
matchUrls();
