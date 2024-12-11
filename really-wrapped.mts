import { lstatSync, readFileSync, readdirSync } from "node:fs";
import chalk from "chalk";
import { resolve } from "node:path";
import { MongoClient, Document } from "mongodb";

interface PlayRecord {
  /**
   * 2024-06-23T09:57:11Z
   */
  ts: string;
  platform: string;
  ms_played: number;
  conn_country: string;
  ip_addr: string;
  master_metadata_track_name: string;
  master_metadata_album_artist_name: string;
  master_metadata_album_album_name: string;
  /**
   * spotify:track:<id>
   **/
  spotify_track_uri: string;
  episode_name: null;
  episode_show_name: null;
  spotify_episode_uri: null;
  reason_start: string;
  reason_end: string;
  shuffle: boolean;
  skipped: boolean;
  offline: boolean;
  offline_timestamp: number;
  incognito_mode: boolean;
}

const uri = "mongodb://localhost:27017";
const dbName = "music";
const collectionName = "imported_plays";

const client = await MongoClient.connect(uri);
const db = client.db(dbName);

async function importPlays() {
  const dir = resolve(process.cwd(), "./spotify_extended_play_history");
  const files = readdirSync(dir, "utf-8");

  for (const file of files) {
    const filePath = resolve(dir, file);
    if (
      !file.endsWith(".json") ||
      !file.includes("Audio") ||
      lstatSync(filePath).isDirectory()
    ) {
      console.log("skipping invalid file", file);
      continue;
    }
    console.log("processing file", file);

    const fileContents = readFileSync(filePath, "utf-8");
    if (!fileContents) {
      console.log("skipping empty file", file);
      continue;
    }

    const plays = JSON.parse(fileContents);

    if (plays.length === 0) {
      console.log("skipping, no records", file);
      continue;
    }

    console.log("inserting records", plays.length);
    // await db
    //   .collection(collectionName)
    //   .insertMany(
    //     plays.map((p: PlayRecord) =>
    //       Object.assign(p, { ts: new Date(p.ts), sourceFile: file, user: '' }),
    //     ),
    //   );
  }
}

const filter = {
  incognito_mode: false,
  ms_played: {
    $gte: 30000,
  },
  spotify_track_uri: {
    $nin: ["spotify:track:1PZoHdDM71mzOFW1T6H03y"],
  },
  ts: {
    $gte: new Date("2024-01-01T00:00:00Z"),
    $lt: new Date("2025-01-01T00:00:00Z"),
  },
};

async function getTopTracksByPlayTime() {
  return await db
    .collection(collectionName)
    .aggregate([
      {
        $match: filter,
      },
      {
        $group:
          /**
           * _id: The id of the group.
           * fieldN: The first field name.
           */
          {
            _id: "$spotify_track_uri",
            total_ms_played: {
              $sum: "$ms_played",
            },
            track_name: {
              $first: "$master_metadata_track_name",
            },
            artist_name: {
              $first: "$master_metadata_album_artist_name",
            },
            album_name: {
              $first: "$master_metadata_album_album_name",
            },
          },
      },

      {
        $sort:
          /**
           * Provide any number of field/order pairs.
           */
          {
            total_ms_played: -1,
          },
      },
      {
        $project: {
          track_name: 1,
          artist_name: 1,
          total_mins_played: {
            $round: [
              {
                $divide: ["$total_ms_played", 60000],
              },
              2,
            ],
          },
          total_hours: {
            $round: [
              {
                $divide: ["$total_ms_played", 3600000],
              },
              2,
            ],
          },
        },
      },
    ])
    .limit(10)
    .toArray();
}

async function getTopTracksByPlayCount() {
  return await db
    .collection(collectionName)
    .aggregate([
      {
        $match: filter,
      },
      {
        $group:
          /**
           * _id: The id of the group.
           * fieldN: The first field name.
           */
          {
            _id: "$spotify_track_uri",
            total_play_count: {
              $sum: 1,
            },
            track_name: {
              $first: "$master_metadata_track_name",
            },
            artist_name: {
              $first: "$master_metadata_album_artist_name",
            },
          },
      },
      {
        $sort:
          /**
           * Provide any number of field/order pairs.
           */
          {
            total_play_count: -1,
          },
      },
    ])
    .limit(10)
    .toArray();
}

async function getTopAlbumsByPlayTime() {
  return await db
    .collection(collectionName)
    .aggregate([
      {
        $match: filter,
      },
      {
        $group: {
          _id: "$master_metadata_album_album_name",
          total_play_time: {
            $sum: "$ms_played",
          },
          total_track_plays_from_album_count: {
            $sum: 1,
          },
          artist_name: {
            $first: "$master_metadata_album_artist_name",
          },
        },
      },
      {
        $project: {
          artist_name: 1,
          total_play_time_mins: {
            $round: [
              {
                $divide: ["$total_play_time", 60000],
              },
              2,
            ],
          },
          total_play_time_hours: {
            $round: [
              {
                $divide: ["$total_play_time", 3600000],
              },
              2,
            ],
          },
        },
      },
      {
        $sort: {
          total_play_time_mins: -1,
        },
      },
    ])
    .limit(10)
    .toArray();
}

async function getTopAlbumsByPlayCount() {
  return await db
    .collection(collectionName)
    .aggregate([
      {
        $match: filter,
      },
      {
        $group: {
          _id: "$master_metadata_album_album_name",
          total_play_time: {
            $sum: "$ms_played",
          },
          total_track_plays_from_album_count: {
            $sum: 1,
          },
          artist_name: {
            $first: "$master_metadata_album_artist_name",
          },
        },
      },
      {
        $sort: {
          total_track_plays_from_album_count: -1,
        },
      },
    ])
    .limit(10)
    .toArray();
}

async function getTopArtistsByPlayTime() {
  return await db
    .collection(collectionName)
    .aggregate([
      {
        $match: filter,
      },
      {
        $group: {
          _id: "$master_metadata_album_artist_name",
          total_play_time: {
            $sum: "$ms_played",
          },
        },
      },
      {
        $sort: {
          total_play_time: -1,
        },
      },
      {
        $project: {
          artist_name: 1,
          total_play_time_mins: {
            $round: [
              {
                $divide: ["$total_play_time", 60000],
              },
              2,
            ],
          },
          total_play_time_hours: {
            $round: [
              {
                $divide: ["$total_play_time", 3600000],
              },
              2,
            ],
          },
        },
      },
    ])
    .limit(10)
    .toArray();
}

async function getTopArtistsByPlayCount() {
  return await db
    .collection(collectionName)
    .aggregate([
      {
        $match: filter,
      },
      {
        $group: {
          _id: "$master_metadata_album_artist_name",
          total_play_count: {
            $sum: 1,
          },
        },
      },
      {
        $sort: {
          total_play_count: -1,
        },
      },
    ])
    .limit(10)
    .toArray();
}

async function getTopDaysByPlayTime() {
  return await db
    .collection(collectionName)
    .aggregate([
      {
        $match: filter,
      },
      {
        $group: {
          _id: {
            $dateToString: {
              format: "%Y-%m-%d",
              date: "$ts",
            },
          },
          total_play_time: {
            $sum: "$ms_played",
          },
        },
      },
      {
        $sort: {
          total_play_time: -1,
        },
      },
      {
        $project: {
          _id: 1,
          total_play_time_mins: {
            $round: [
              {
                $divide: ["$total_play_time", 60000],
              },
              2,
            ],
          },
          total_play_time_hours: {
            $round: [
              {
                $divide: ["$total_play_time", 3600000],
              },
              2,
            ],
          },
        },
      },
    ])
    .limit(10)
    .toArray();
}

async function getTopDaysByPlayCount() {
  return await db
    .collection(collectionName)
    .aggregate([
      {
        $match: filter,
      },
      {
        $group: {
          _id: {
            $dateToString: {
              format: "%Y-%m-%d",
              date: "$ts",
            },
          },
          total_play_count: {
            $sum: 1,
          },
        },
      },
      {
        $sort: {
          total_play_count: -1,
        },
      },
      {
        $project: {
          _id: 1,
          total_play_count: 1,
        },
      },
    ])
    .limit(10)
    .toArray();
}

const gold = chalk.bgHex("EFBF04");
const silver = chalk.bgHex("C0C0C0");
const bronze = chalk.bgHex("CD7F32");
function logSeries(title = "", records: any[], fmt: (r: any) => string) {
  console.log(chalk.bgBlue(title) + `\n`);
  records.forEach((r, idx) => {
    const rank = idx + 1;
    const line = `${rank}. ${fmt(r)}`;
    switch (rank) {
      case 1:
        console.log(gold(line));
        break;
      case 2:
        console.log(chalk.black(silver(line)));
        break;
      case 3:
        console.log(bronze(line));
        break;
      default:
        console.log(line);
        break;
    }
  });
  console.log(`\n`);
}

async function getTotalMinsPlayed() {
  const [r] = await db
    .collection(collectionName)
    .aggregate([
      {
        $match: filter,
      },
      {
        $group: {
          _id: null,
          total_ms_played: {
            $sum: "$ms_played",
          },
        },
      },
      {
        $project: {
          total_mins_played: {
            $round: [
              {
                $divide: ["$total_ms_played", 60000],
              },
              2,
            ],
          },
        },
      },
    ])
    .toArray();

  return r.total_mins_played;
}

async function getTotalUniqueTracksPlayed() {
  const [r] = await db
    .collection(collectionName)
    .aggregate([
      {
        $match: filter,
      },
      {
        $group: {
          _id: "$spotify_track_uri",
        },
      },
      { $count: "unique_tracks_played" },
    ])
    .toArray();
  return r.unique_tracks_played;
}

console.log(
  chalk.bgGreen(
    `You listened to ${chalk.bold(await getTotalMinsPlayed())} mins of music across ${chalk.bold(await getTotalUniqueTracksPlayed())} tracks.`,
  ),
);
console.log(`\n`);

logSeries(
  "Top Tracks by Time Listened",
  await getTopTracksByPlayTime(),
  (r) => `${r.total_mins_played} mins - ${r.track_name} by ${r.artist_name}`,
);

logSeries(
  "Top Artists by Time Listened",
  await getTopArtistsByPlayTime(),
  (r) => `${r.total_play_time_mins} mins - ${r._id}`,
);

logSeries(
  "Top Albums by Time Listened",
  await getTopAlbumsByPlayTime(),
  (r) => `${r.total_play_time_mins} mins - ${r._id} by ${r.artist_name}`,
);

logSeries(
  "Top Days by Time Listened",
  await getTopDaysByPlayTime(),
  (r) => `${r.total_play_time_mins} mins - ${r._id}`,
);

logSeries(
  "Top Tracks by Play Count",
  await getTopTracksByPlayCount(),
  (r) => `${r.total_play_count} plays - ${r.track_name} by ${r.artist_name}`,
);

logSeries(
  "Top Artists by Play Count",
  await getTopArtistsByPlayCount(),
  (r) => `${r.total_play_count} plays - ${r._id}`,
);
logSeries(
  "Top Albums by Play Count",
  await getTopAlbumsByPlayCount(),
  (r) =>
    `${r.total_track_plays_from_album_count} songs played from ${r._id} by ${r.artist_name}`,
);

logSeries(
  "Top Days by Play Count",
  await getTopDaysByPlayCount(),
  (r) => `${r.total_play_count} plays - ${r._id}`,
);
// idk why the process is hanging, maybe an issue with top level await and tsx
process.exit(0);
