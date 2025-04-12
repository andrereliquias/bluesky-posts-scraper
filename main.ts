import * as fs from "fs";
import * as path from "path";
import fetch from "node-fetch";
import archiver from "archiver";
import { URLSearchParams } from "url";
import * as config from "./config.json";

const QUERY = config.query;
const SINCE = config.since;
const UNTIL = config.until;
const LANG = config.language;
const LIMIT = config.limit;
const POSTS_PER_FILE = config.postsPerFile;
const BASE_DIR = path.join(__dirname, config.baseFilesDir);
const minuteInterval: number = config.minuteInterval;

if (!fs.existsSync(BASE_DIR)) {
  fs.mkdirSync(BASE_DIR, { recursive: true });
  console.log(`Diretório criado: ${BASE_DIR}`);
}

interface Post {
  uri: string;
  cid: string;
  author: {
    did: string;
    handle: string; // Username
    displayName: string;
    avatar: string;
    associated: {
      chat: {
        allowIncoming: string;
      };
    };
    labels: {
      src: string;
      uri: string;
      cid: string;
      val: string;
      cts: string;
    }[];
    createdAt: string;
  };
  record: {
    $type: string;
    createdAt: string; // Post date
    langs: string[];
    embed: any;
    reply: {
      parent: {
        uri: string;
        cid: string;
      };
      root: {
        uri: string;
        cid: string;
      };
    };
    text: string; // Post content
  };
  replyCount: number;
  repostCount: number;
  likeCount: number;
  quoteCount: number;
  indexedAt: string;
  labels: any;
}

interface SearchPostsResponse {
  cursor?: string;
  posts: Post[];
}

/**
 * Function to record messages incrementally in the `runtime.log` file.
 *
 * @param message Message to be logged
 * @returns void
 */
function logMessage(message: string): void {
  const timestamp = new Date().toISOString();
  fs.appendFileSync("runtime.log", `${timestamp} - ${message}\n`);
}

/**
 * Function that calls the API to fetch a page of posts.
 *
 * @param query Search string
 * @param since Start date
 * @param until End date
 * @param lang Language
 * @param limit Number of posts
 * @param cursor When there is more than one page, the cursor is used to navigate between them
 *
 * @returns API response with the posts
 */
async function fetchPosts(
  query: string,
  since: string,
  until: string,
  lang: string,
  limit: number,
  cursor?: string
): Promise<SearchPostsResponse> {
  const baseUrl = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchposts";

  const urlParams = new URLSearchParams({
    q: query,
    sort: "latest",
    since,
    until,
    lang,
    limit: limit.toString(),
  });

  if (cursor) {
    urlParams.append("cursor", cursor);
  }

  const url = `${baseUrl}?${urlParams.toString()}`;
  logMessage(`API call: ${url}`);
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`Erro HTTP ${response.status}`);
  }

  const data = (await response.json()) as SearchPostsResponse;
  return data;
}

let totalPostsProcessed = 0;
let currentCsvStream: fs.WriteStream | null = null;
let currentFilePostCount = 0;
let currentFileFirstCreatedAt: string | null = null;
let currentFileLastCreatedAt: string | null = null;
let fileIndex = 1;

/**
 * Create a new (temporary) CSV file and write the header.
 *
 * @returns void
 */
function createCSVFile(): void {
  const tempFileName = path.join(BASE_DIR, `posts_temp_${fileIndex}.csv`);
  currentCsvStream = fs.createWriteStream(tempFileName, { flags: "w" });
  currentCsvStream.write(
    "author.handle,record.createdAt,record.text,replyCount,repostCount,likeCount,quoteCount\n"
  );
  currentFilePostCount = 0;
  currentFileFirstCreatedAt = null;
  currentFileLastCreatedAt = null;
  logMessage(`Creating new CSV file: ${tempFileName}`);
}

/**
 * Escapes values to CSV (commas, quotation marks, line breaks).
 *
 * @param value Text to be escaped
 * @returns Normalized text for CSV
 */
function escapeCSV(value: string): string {
  if (value.includes(",") || value.includes('"') || value.includes("\n")) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

/**
 * Finishes the processing of the CSV file and zips the file.
 *
 * @returns Promise<void>
 */
function flushCSVFile(): Promise<void> {
  return new Promise((resolve, reject) => {
    if (!currentCsvStream) {
      resolve();
      return;
    }
    const tempFileName = path.join(BASE_DIR, `posts_temp_${fileIndex}.csv`);
    currentCsvStream.end(() => {
      const safeFirst = currentFileFirstCreatedAt
        ? currentFileFirstCreatedAt.replace(/[:T\-Z]/g, "")
        : "start";
      const safeLast = currentFileLastCreatedAt
        ? currentFileLastCreatedAt.replace(/[:T\-Z]/g, "")
        : "end";
      const finalFileName = path.join(
        BASE_DIR,
        `posts_${safeFirst}_${safeLast}.csv`
      );
      fs.renameSync(tempFileName, finalFileName);
      logMessage(`CSV file finished: ${finalFileName}`);

      const zipFileName = `${finalFileName}.zip`;
      const output = fs.createWriteStream(zipFileName);
      const archive = archiver("zip", { zlib: { level: 9 } });
      output.on("close", () => {
        logMessage(
          `ZIP file created: ${zipFileName} (${archive.pointer()} bytes)`
        );

        try {
          fs.unlinkSync(finalFileName);
          logMessage(`CSV file removed after compression: ${finalFileName}`);
        } catch (err) {
          logMessage(`CSV file failed to be removed ${finalFileName}: ${err}`);
        }

        fileIndex++;
        currentCsvStream = null;
        currentFilePostCount = 0;
        currentFileFirstCreatedAt = null;
        currentFileLastCreatedAt = null;
        resolve();
      });
      archive.on("error", (err) => reject(err));
      archive.pipe(output);
      archive.file(finalFileName, { name: path.basename(finalFileName) });
      archive.finalize();
    });
  });
}

/**
 * Processes the posts returned by the endpoint call
 * - Creates a temporary CSV file
 * - Extracts the desired fields
 * - When the post limit is reached, ends the file and creates a new one
 *
 * @param posts Posts to be processed
 * @returns Promise<void>
 */
async function processPosts(posts: Post[]): Promise<void> {
  if (!currentCsvStream) {
    createCSVFile();
  }
  for (const post of posts) {
    const handle = post.author.handle;
    const createdAt = post.record.createdAt;
    const text = post.record.text.replace(/\r?\n|\r/g, " ");
    const replyCount = post.replyCount;
    const repostCount = post.repostCount;
    const likeCount = post.likeCount;
    const quoteCount = post.quoteCount;

    if (!currentFileFirstCreatedAt) {
      currentFileFirstCreatedAt = createdAt;
    }
    currentFileLastCreatedAt = createdAt;

    const csvLine =
      [
        escapeCSV(handle),
        escapeCSV(createdAt),
        escapeCSV(text),
        replyCount,
        repostCount,
        likeCount,
        quoteCount,
      ].join(",") + "\n";

    currentCsvStream.write(csvLine);
    totalPostsProcessed++;
    currentFilePostCount++;

    if (currentFilePostCount >= POSTS_PER_FILE) {
      await flushCSVFile();
      createCSVFile();
    }
  }
}

/**
 * Makes the API calls for a given interval (in minutes),
 * using pagination (cursor) to get all the posts in that interval.
 *
 * @param since Timestamp of start of interval
 * @param until Timestamp of end of interval
 */
async function fetchPostsForPeriod(
  since: string,
  until: string
): Promise<void> {
  let cursor: string | undefined = undefined;
  do {
    const result = await fetchPosts(QUERY, since, until, LANG, LIMIT, cursor);
    logMessage(
      `API call for the interval ${since} - ${until}: cursor=${cursor}, returned ${
        result.posts?.length || 0
      } posts.`
    );
    if (result.posts && result.posts.length > 0) {
      await processPosts(result.posts);
    } else {
      logMessage(`No posts returned for the period ${since} - ${until}.`);
    }
    cursor = result.cursor;
  } while (cursor);
}

/**
 * Returns a date in "YYYY-MM-DD" format from a Date object.
 *
 * @param date Date object
 * @returns Formatted date
 */
function getDateString(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

/**
 * Returns the date of the next day, given a string "YYYY-MM-DD".
 *
 * @param dateStr Date in "YYYY-MM-DD" format
 * @returns String with next day's date
 */
function getNextDay(dateStr: string): string {
  const [year, month, day] = dateStr.split("-").map(Number);
  const dateObj = new Date(year, month - 1, day);
  dateObj.setDate(dateObj.getDate() + 1);
  return getDateString(dateObj);
}

async function main() {
  try {
    const startDayStr = SINCE.split("T")[0];
    const endDayStr = UNTIL.split("T")[0];
    let currentDateStr = startDayStr;

    while (currentDateStr <= endDayStr) {
      // Divide o dia em intervalos de minuteInterval minutos (24 horas = 1440 minutos)
      for (
        let totalMinutes = 0;
        totalMinutes < 1440;
        totalMinutes += minuteInterval
      ) {
        const startHour = Math.floor(totalMinutes / 60);
        const startMinute = totalMinutes % 60;
        let endTotalMinutes = totalMinutes + minuteInterval - 1;
        if (endTotalMinutes >= 1440) {
          endTotalMinutes = 1439; // máximo: 23:59
        }

        const endHour = Math.floor(endTotalMinutes / 60);
        const endMinute = endTotalMinutes % 60;

        const intervalSince = `${currentDateStr}T${String(startHour).padStart(
          2,
          "0"
        )}:${String(startMinute).padStart(2, "0")}:00-03:00`;
        const intervalUntil = `${currentDateStr}T${String(endHour).padStart(
          2,
          "0"
        )}:${String(endMinute).padStart(2, "0")}:59-03:00`;
        logMessage(
          `Processing interval: ${intervalSince} até ${intervalUntil}`
        );
        await fetchPostsForPeriod(intervalSince, intervalUntil);
      }
      currentDateStr = getNextDay(currentDateStr);
    }

    if (currentCsvStream && currentFilePostCount > 0) {
      await flushCSVFile();
    }
    logMessage(
      `Processing completed. Total saved posts: ${totalPostsProcessed}`
    );
  } catch (error) {
    logMessage(`Something went wrong: ${error}`);
    console.error("Erro:", error);
  }
}

main();
