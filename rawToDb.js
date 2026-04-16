import { neon } from "@neondatabase/serverless";
import fs from "fs";
import { parse } from "csv-parse/sync";
import "dotenv/config";
import { chain } from "stream-chain";
import { parser } from "stream-json";
import { streamArray } from "stream-json/streamers/stream-array.js";
import {
    CURRENT_TEAMS,
    dateOrNull,
    floatOrNull,
    intOrNull,
    intOrZero,
    normalizePositions,
    normalizeTeam,
    nullIfEmpty,
} from "./helpers.js";

const sql = neon(process.env.DATABASE_URL);
const BATCH = 300;

// ─── Load source files ─────────────────────────────────────────────

const profiles = JSON.parse(fs.readFileSync("./data/player_profiles.json", "utf8"));
const teamSeasons = parse(fs.readFileSync("./data/team_seasons.csv", "utf8"), {
    columns: true,
    skip_empty_lines: true,
    trim: true,
});

// ─── 1. Seed teams ────────────────────────────────────────────────

async function seedTeams() {
    await Promise.all(
        CURRENT_TEAMS.map(
            ([code, name, city]) =>
                sql`INSERT INTO team (team_code, team_name, city)
                VALUES (${code}, ${name}, ${city})
                ON CONFLICT (team_code) DO NOTHING`,
        ),
    );
    console.log(`✓ Seeded 32 teams`);
}

// ─── 2. Insert players (rows first, then positions) ───────────────

async function insertPlayers() {
    // Phase 1: insert all player rows
    for (let i = 0; i < profiles.length; i += BATCH) {
        const batch = profiles.slice(i, i + BATCH);
        await Promise.all(
            batch.map((p) => {
                const draftTeam = normalizeTeam(p.draft_team, null);
                const currentTeam = normalizeTeam(p.current_team, null);
                return sql`
              INSERT INTO player (
                player_id, name, height, weight,
                birth_date, birth_place, death_date,
                college, high_school,
                draft_team_code, draft_round, draft_position, draft_year,
                current_team_code, current_salary, hof_induction_year
              )
              VALUES (
                ${p.player_id},
                ${nullIfEmpty(p.name)},
                ${nullIfEmpty(p.height)},
                ${intOrNull(p.weight)},
                ${dateOrNull(p.birth_date)},
                ${nullIfEmpty(p.birth_place)},
                ${dateOrNull(p.death_date)},
                ${nullIfEmpty(p.college)},
                ${nullIfEmpty(p.high_school)},
                ${draftTeam},
                ${intOrNull(p.draft_round)},
                ${intOrNull(p.draft_position)},
                ${intOrNull(p.draft_year)},
                ${currentTeam},
                ${floatOrNull(p.current_salary)},
                ${intOrNull(p.hof_induction_year)}
              )
              ON CONFLICT (player_id) DO NOTHING`;
            }),
        );
        if ((i + BATCH) % 1000 < BATCH)
            console.log(`  … ${Math.min(i + BATCH, profiles.length)} / ${profiles.length} players`);
    }

    // Phase 2: insert all positions (FKs now satisfied)
    const positionRows = [];
    for (const p of profiles) {
        for (const pos of normalizePositions(p.position)) {
            positionRows.push({ player_id: p.player_id, position: pos });
        }
    }
    for (let i = 0; i < positionRows.length; i += BATCH) {
        const batch = positionRows.slice(i, i + BATCH);
        await Promise.all(
            batch.map(
                (r) =>
                    sql`INSERT INTO player_position (player_id, position)
                VALUES (${r.player_id}, ${r.position})
                ON CONFLICT DO NOTHING`,
            ),
        );
    }

    console.log(`✓ Inserted ${profiles.length} players, ${positionRows.length} position rows`);
}

// ─── 3. Insert game logs ───────────────────────────────────────────

async function flushBatch(batch, retries = 3) {
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            await Promise.all(batch);
            return;
        } catch (err) {
            if (attempt === retries) throw err;
            console.warn(`  ⚠ batch failed (attempt ${attempt}/${retries}), retrying in 2s…`);
            await new Promise((r) => setTimeout(r, 2000));
            // queries are already promises that fired — rebuild them isn't possible,
            // but ON CONFLICT DO NOTHING means we can just re-run the whole batch
        }
    }
}

async function insertGameLogs() {
    // Check how many rows already exist so we can skip ahead in the stream
    const [{ count: existingCount }] = await sql`SELECT count(*)::int as count FROM game_log`;
    if (existingCount > 0) {
        console.log(`  ℹ ${existingCount} game_log rows already exist, skipping ahead in stream…`);
    }

    let streamed = 0;
    let inserted = 0;
    let skipped = 0;

    const pipeline = chain([fs.createReadStream("./data/player_games.json"), parser(), streamArray()]);

    let batch = [];
    for await (const { value: g } of pipeline) {
        // Skip rows we've already inserted — much faster than re-sending to DB
        streamed++;
        if (streamed <= existingCount) {
            if (streamed % 100000 === 0) console.log(`  … skipping ${streamed} / ${existingCount}`);
            continue;
        }

        const teamCode = normalizeTeam(g.team, g.year);
        const opponentCode = normalizeTeam(g.opponent, g.year);

        if (!teamCode || !opponentCode) {
            skipped++;
            continue;
        }

        batch.push(sql`
          INSERT INTO game_log (
            player_id, team_code, opponent_code,
            year, game_date, game_number, age,
            game_location, game_won,
            player_team_score, opponent_score,
            passing_attempts, passing_completions, passing_yards,
            passing_rating, passing_touchdowns, passing_interceptions,
            passing_sacks, passing_sacks_yards_lost,
            rushing_attempts, rushing_yards, rushing_touchdowns,
            receiving_targets, receiving_receptions, receiving_yards, receiving_touchdowns,
            kick_return_attempts, kick_return_yards, kick_return_touchdowns,
            punt_return_attempts, punt_return_yards, punt_return_touchdowns,
            defense_sacks, defense_tackles, defense_tackle_assists,
            defense_interceptions, defense_interception_yards, defense_interception_touchdowns,
            defense_safeties,
            point_after_attempts, point_after_makes,
            field_goal_attempts, field_goal_makes,
            punting_attempts, punting_yards, punting_blocked
          )
          VALUES (
            ${g.player_id}, ${teamCode}, ${opponentCode},
            ${intOrNull(g.year)}, ${g.date}, ${intOrNull(g.game_number)}, ${nullIfEmpty(g.age)},
            ${g.game_location}, ${g.game_won},
            ${intOrNull(g.player_team_score)}, ${intOrNull(g.opponent_score)},
            ${g.passing_attempts}, ${g.passing_completions}, ${g.passing_yards},
            ${floatOrNull(g.passing_rating)}, ${g.passing_touchdowns}, ${g.passing_interceptions},
            ${g.passing_sacks}, ${g.passing_sacks_yards_lost},
            ${g.rushing_attempts}, ${g.rushing_yards}, ${g.rushing_touchdowns},
            ${g.receiving_targets}, ${g.receiving_receptions}, ${g.receiving_yards}, ${g.receiving_touchdowns},
            ${g.kick_return_attempts}, ${g.kick_return_yards}, ${g.kick_return_touchdowns},
            ${g.punt_return_attempts}, ${g.punt_return_yards}, ${g.punt_return_touchdowns},
            ${floatOrNull(g.defense_sacks)}, ${g.defense_tackles}, ${g.defense_tackle_assists},
            ${g.defense_interceptions}, ${g.defense_interception_yards}, ${g.defense_interception_touchdowns},
            ${g.defense_safeties},
            ${g.point_after_attemps}, ${g.point_after_makes},
            ${g.field_goal_attempts}, ${g.field_goal_makes},
            ${g.punting_attempts}, ${g.punting_yards}, ${g.punting_blocked}
          )
          ON CONFLICT DO NOTHING`);
        inserted++;

        if (batch.length >= BATCH) {
            await flushBatch(batch);
            batch = [];
            if (inserted % 10000 === 0) console.log(`  … ${inserted} game logs`);
        }
    }
    if (batch.length) await flushBatch(batch);

    console.log(`✓ Inserted ${inserted} game log rows (${skipped} skipped)`);
}

// ─── 4. Insert team seasons ────────────────────────────────────────

async function insertTeamSeasons() {
    let inserted = 0;
    let skipped = 0;
    const batch = [];

    for (const ts of teamSeasons) {
        const teamCode = normalizeTeam(ts.team, ts.year);

        if (!teamCode) {
            skipped++;
            continue;
        }

        batch.push(sql`
          INSERT INTO team_season (
            team_code, year, wins, losses, ties, win_loss_perc,
            points_for, points_against, points_diff, mov, games_played,
            total_yards, plays_offense, yds_per_play_offense,
            turnovers, fumbles_lost, first_downs,
            pass_completions, pass_attempts, pass_yards, pass_touchdowns,
            pass_interceptions, pass_net_yds_per_att, pass_first_downs,
            rush_attempts, rush_yards, rush_touchdowns, rush_yds_per_att, rush_first_downs,
            penalties, penalty_yards, penalty_first_downs,
            score_pct, turnover_pct, exp_pts_total
          )
          VALUES (
            ${teamCode}, ${intOrNull(ts.year)},
            ${intOrZero(ts.wins)}, ${intOrZero(ts.losses)}, ${intOrZero(ts.ties)},
            ${floatOrNull(ts.win_loss_perc)},
            ${intOrNull(ts.points)}, ${intOrNull(ts.points_opp)}, ${intOrNull(ts.points_diff)},
            ${floatOrNull(ts.mov)}, ${intOrNull(ts.g)},
            ${intOrNull(ts.total_yards)}, ${intOrNull(ts.plays_offense)},
            ${floatOrNull(ts.yds_per_play_offense)},
            ${intOrNull(ts.turnovers)}, ${intOrNull(ts.fumbles_lost)},
            ${intOrNull(ts.first_down)},
            ${intOrNull(ts.pass_cmp)}, ${intOrNull(ts.pass_att)},
            ${intOrNull(ts.pass_yds)}, ${intOrNull(ts.pass_td)},
            ${intOrNull(ts.pass_int)}, ${floatOrNull(ts.pass_net_yds_per_att)},
            ${intOrNull(ts.pass_fd)},
            ${intOrNull(ts.rush_att)}, ${intOrNull(ts.rush_yds)}, ${intOrNull(ts.rush_td)},
            ${floatOrNull(ts.rush_yds_per_att)}, ${intOrNull(ts.rush_fd)},
            ${intOrNull(ts.penalties)}, ${intOrNull(ts.penalties_yds)}, ${intOrNull(ts.pen_fd)},
            ${floatOrNull(ts.score_pct)}, ${floatOrNull(ts.turnover_pct)},
            ${floatOrNull(ts.exp_pts_tot)}
          )
          ON CONFLICT (team_code, year) DO NOTHING`);
        inserted++;

        if (batch.length >= BATCH) {
            await Promise.all(batch.splice(0));
        }
    }
    if (batch.length) await Promise.all(batch);

    console.log(`✓ Inserted ${inserted} team season rows (${skipped} skipped)`);
}

// ─── Run ───────────────────────────────────────────────────────────

async function main() {
    try {
        await seedTeams();
        await insertPlayers();
        await insertGameLogs();
        await insertTeamSeasons();
        console.log("✓ Done");
    } catch (err) {
        console.error("✗ Seed failed:", err);
        process.exit(1);
    }
}

main();
