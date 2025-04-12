# Bluesky Posts Scraper

This project consist of a script that captures posts from the Bluesky social network over a specified time range using a search string. The script utilizes the `app.bsky.feed.searchPosts` endpoint. You can find the complete documentation [here](https://docs.bsky.app/docs/api/app-bsky-feed-search-posts).

## How It Works

Given a date range and a time interval (in minutes), the script iterates through the entire period and makes an API call every _n_ minutes. In addition, if there are more posts to capture within a given interval, the script continues making requests while a cursor is provided, ensuring that all posts in that interval are collected. The captured posts are saved into a CSV file.
Furthermore, once a specified number of posts has been collected, the script will finalize (and compress) the current CSV file and start a new file to store subsequent posts.

### Setup

1. Open the `config.json` file and set the following configuration variables:

   - **query**: The search string.
   - **language**: The language code.
   - **since**: The start date (with time and timezone).
   - **until**: The end date (with time and timezone).
   - **minuteInterval**: The interval (in minutes) for each search iteration.
   - **limit**: The maximum number of posts returned per API call.
   - **postsPerFile**: The maximum number of posts to include before compressing the file.
   - **baseFilesDir**: The base directory where the files will be saved.

2. Install the required packages using npm:

   ```bash
   npm install
   ```

3. Run the script:

   ```bash
   npm run start
   ```

4. Monitor the progress through the logs in the `runtime.log` file.
