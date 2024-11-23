const axios = require('axios');
const cheerio = require('cheerio');
const fs = require('fs');
const csvParser = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const xml2js = require('xml2js');

// Flags to control functionality
const ENABLE_SITEMAP = false; // Enable/Disable sitemap crawling
const ENABLE_CANONICAL = false; // Enable/Disable canonical URL extraction

// A Set to store unique URLs
let visitedUrls = new Set();

// Concurrency limit
const MAX_CONCURRENT_REQUESTS = 5;
let activeRequests = 0;
const requestQueue = [];

// Maximum depth for crawling
const MAX_DEPTH = 10;

// Helper function to normalize URLs (removing query strings and hash fragments)
function normalizeUrl(urlString) {
  try {
    const parsedUrl = new URL(urlString);
    parsedUrl.search = ''; // Remove query string
    parsedUrl.hash = '';   // Remove hash
    return parsedUrl.href;
  } catch (error) {
    console.error(`Failed to normalize URL: ${urlString}`);
    return null;
  }
}

// Function to load URLs from an existing CSV file into a Set
function loadVisitedUrls(filePath) {
  const visited = new Set();

  if (fs.existsSync(filePath)) {
    console.log(`Loading existing URLs from ${filePath}...`);
    return new Promise((resolve, reject) => {
      fs.createReadStream(filePath)
        .pipe(csvParser())
        .on('data', (row) => {
          if (row.URL) {
            visited.add(row.URL); // Add the URL to the Set
          }
        })
        .on('end', () => {
          console.log(`Loaded ${visited.size} URLs from ${filePath}`);
          resolve(visited);
        })
        .on('error', (err) => {
          console.error(`Error reading ${filePath}:`, err);
          reject(err);
        });
    });
  } else {
    console.log(`No existing file found at ${filePath}. Starting fresh.`);
    return Promise.resolve(visited);
  }
}

// Function to check if a URL is on the same domain
function isSameDomain(urlString, baseUrl) {
  try {
    const urlObj = new URL(urlString);
    const baseObj = new URL(baseUrl);
    return urlObj.hostname === baseObj.hostname;
  } catch (error) {
    console.error(`Error checking domain for URL: ${urlString}`);
    return false;
  }
}

// Function to extract all links from a page
async function extractLinks(pageUrl) {
  try {
    const response = await axios.get(pageUrl);
    const $ = cheerio.load(response.data);
    const links = [];

    $('a').each((_, element) => {
      const href = $(element).attr('href');
      if (href) {
        let absoluteUrl;
        try {
          absoluteUrl = new URL(href, pageUrl).href;
        } catch (e) {
          return; // Skip invalid URLs
        }

        const normalizedUrl = normalizeUrl(absoluteUrl);
        if (normalizedUrl && isSameDomain(normalizedUrl, pageUrl)) {
          links.push(normalizedUrl);
        }
      }
    });

    return links;
  } catch (error) {
    console.error(`Error fetching ${pageUrl}: ${error.message}`);
    return [];
  }
}

// Recursive crawler function with depth control
async function crawl(pageUrl, depth) {
  if (depth > MAX_DEPTH) return;

  const canonicalUrl = ENABLE_CANONICAL ? await getCanonicalUrl(pageUrl) : normalizeUrl(pageUrl);
  if (!canonicalUrl || visitedUrls.has(canonicalUrl)) return;

  visitedUrls.add(canonicalUrl);
  console.log(`Crawling: ${canonicalUrl} (Depth: ${depth})`);

  const links = await extractLinks(pageUrl);

  // Add links to the request queue for concurrent processing
  for (const link of links) {
    if (!visitedUrls.has(link)) {
      requestQueue.push(() => crawl(link, depth + 1));
    }
  }

  processQueue();
}

// Function to process the request queue with concurrency limit
function processQueue() {
  while (activeRequests < MAX_CONCURRENT_REQUESTS && requestQueue.length > 0) {
    const nextRequest = requestQueue.shift();
    activeRequests++;
    nextRequest()
      .then(() => {
        activeRequests--;
        processQueue();
      })
      .catch((error) => {
        console.error(error);
        activeRequests--;
        processQueue();
      });
  }
}

// Function to save URLs to a CSV
function saveToCsv(filePath, urls) {
  const csvWriter = createCsvWriter({
    path: filePath,
    header: [{ id: 'url', title: 'URL' }]
  });

  const records = Array.from(urls).map((url) => ({ url }));

  csvWriter
    .writeRecords(records)
    .then(() => console.log(`Saved ${records.length} URLs to ${filePath}`));
}

// Entry point
(async () => {
  const websiteUrl = process.argv[2];
  if (!websiteUrl) {
    console.error('Please provide a website URL as an argument.');
    console.error('Usage: node crawler.js <website_url>');
    process.exit(1);
  }

  const outputFilePath = './output/crawled_urls.csv';

  // Load existing URLs
  visitedUrls = await loadVisitedUrls(outputFilePath);

  console.log(`Starting crawl on: ${websiteUrl}`);

  requestQueue.push(() => crawl(websiteUrl, 0));
  processQueue();

  // Wait for all requests to complete
  const interval = setInterval(() => {
    if (activeRequests === 0 && requestQueue.length === 0) {
      clearInterval(interval);
      saveToCsv(outputFilePath, visitedUrls); // Save all crawled URLs
    }
  }, 100); // Check every 100ms
})();
