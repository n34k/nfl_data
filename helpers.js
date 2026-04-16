// ─── Helpers ───────────────────────────────────────────────────────

export const nullIfEmpty = (v) => (v === "" || v === "null" || v == null ? null : v);
export const intOrNull = (v) => (nullIfEmpty(v) === null ? null : parseInt(v, 10));
export const floatOrNull = (v) => (nullIfEmpty(v) === null ? null : parseFloat(v));
export const intOrZero = (v) => intOrNull(v) ?? 0;

// Sanitize dates like "1962-08-00" → null (day 00 is invalid in Postgres)
export const dateOrNull = (v) => {
    const s = nullIfEmpty(v);
    if (!s) return null;
    if (/-00/.test(s)) return null; // month-00 or day-00
    return s;
};

// ─── Team normalization ────────────────────────────────────────────
// Maps both legacy 3-letter codes AND full CSV names to the current
// canonical 3-letter code for that franchise

const TEAM_LOOKUP = {
    // ── current codes (pass-through) ──
    ARI: "ARI",
    ATL: "ATL",
    BAL: "BAL",
    BUF: "BUF",
    CAR: "CAR",
    CHI: "CHI",
    CIN: "CIN",
    CLE: "CLE",
    DAL: "DAL",
    DEN: "DEN",
    DET: "DET",
    GNB: "GNB",
    HOU: "HOU",
    IND: "IND",
    JAX: "JAX",
    KAN: "KAN",
    LVR: "LVR",
    LAC: "LAC",
    LAR: "LAR",
    MIA: "MIA",
    MIN: "MIN",
    NWE: "NWE",
    NOR: "NOR",
    NYG: "NYG",
    NYJ: "NYJ",
    PHI: "PHI",
    PIT: "PIT",
    SFO: "SFO",
    SEA: "SEA",
    TAM: "TAM",
    TEN: "TEN",
    WAS: "WAS",

    // ── simple legacy codes ──
    OAK: "LVR", // Oakland Raiders → Las Vegas Raiders
    RAI: "LVR", // LA Raiders → Las Vegas Raiders
    SDG: "LAC", // San Diego Chargers → LA Chargers
    RAM: "LAR", // LA Rams (original) → LA Rams
    PHO: "ARI", // Phoenix Cardinals → Arizona Cardinals
    CRD: "ARI", // Chicago/StL Cardinals → Arizona Cardinals
    BOS: "NWE", // Boston Patriots → New England Patriots
    DTX: "KAN", // Dallas Texans → Kansas City Chiefs
    NYT: "NYJ", // New York Titans → New York Jets
    NYY: "IND", // New York Yanks → (Dallas Texans →) Baltimore/Indianapolis Colts

    // ── full CSV name mappings (for team_season) ──
    "Arizona Cardinals": "ARI",
    "Atlanta Falcons": "ATL",
    "Baltimore Ravens": "BAL",
    "Buffalo Bills": "BUF",
    "Carolina Panthers": "CAR",
    "Chicago Bears": "CHI",
    "Cincinnati Bengals": "CIN",
    "Cleveland Browns": "CLE",
    "Dallas Cowboys": "DAL",
    "Denver Broncos": "DEN",
    "Detroit Lions": "DET",
    "Green Bay Packers": "GNB",
    "Houston Texans": "HOU",
    "Indianapolis Colts": "IND",
    "Jacksonville Jaguars": "JAX",
    "Kansas City Chiefs": "KAN",
    "Las Vegas Raiders": "LVR",
    "Los Angeles Chargers": "LAC",
    "Los Angeles Rams": "LAR",
    "Miami Dolphins": "MIA",
    "Minnesota Vikings": "MIN",
    "New England Patriots": "NWE",
    "New Orleans Saints": "NOR",
    "New York Giants": "NYG",
    "New York Jets": "NYJ",
    "Philadelphia Eagles": "PHI",
    "Pittsburgh Steelers": "PIT",
    "San Francisco 49ers": "SFO",
    "Seattle Seahawks": "SEA",
    "Tampa Bay Buccaneers": "TAM",
    "Tennessee Titans": "TEN",
    "Washington Commanders": "WAS",
    "Oakland Raiders": "LVR",
    "San Diego Chargers": "LAC",
    "St. Louis Rams": "LAR",
    "Washington Redskins": "WAS",
    "Washington Football Team": "WAS",
    "Chicago Cardinals": "ARI",
    "St. Louis Cardinals": "ARI",
    "Phoenix Cardinals": "ARI",
    "Houston Oilers": "TEN",
    "Baltimore Colts": "IND",
    "Boston Yanks": "IND",
    "Brooklyn Dodgers": "IND",
    "Boston Patriots": "NWE",
    "Dallas Texans": "KAN",
    "Boston Redskins": "WAS",
    "Tennessee Oilers": "TEN",
    "Cleveland Rams": "LAR",
    "Los Angeles Raiders": "LVR",
    "Pittsburgh Pirates": "PIT",
    "New York Titans": "NYJ",
    "Bos/Bkn Yanks/Tigers": "IND",
    "Chi/Pit Cards/Steelers": "ARI",
    "Phi/Pit Eagles/Steelers": "PHI",
    "New York Bulldogs": "IND",
    "New York Yanks": "IND",
};

// STL is year-dependent — Cardinals pre-1988, Rams 1995+
export const normalizeTeam = (code, year) => {
    if (code === "STL") {
        const y = year ? parseInt(year, 10) : null;
        if (y && y <= 1987) return "ARI";
        return "LAR";
        // 1988–1994 gap — STL Cardinals moved to PHO in 1988
        // so this range shouldn't appear, but guard it anyway
    }
    const mapped = TEAM_LOOKUP[code?.trim()];
    if (!mapped) console.warn(`⚠ Unknown team code: "${code}"`);
    return mapped ?? null;
};

export const CURRENT_TEAMS = [
    ["ARI", "Arizona Cardinals", "Phoenix"],
    ["ATL", "Atlanta Falcons", "Atlanta"],
    ["BAL", "Baltimore Ravens", "Baltimore"],
    ["BUF", "Buffalo Bills", "Buffalo"],
    ["CAR", "Carolina Panthers", "Charlotte"],
    ["CHI", "Chicago Bears", "Chicago"],
    ["CIN", "Cincinnati Bengals", "Cincinnati"],
    ["CLE", "Cleveland Browns", "Cleveland"],
    ["DAL", "Dallas Cowboys", "Dallas"],
    ["DEN", "Denver Broncos", "Denver"],
    ["DET", "Detroit Lions", "Detroit"],
    ["GNB", "Green Bay Packers", "Green Bay"],
    ["HOU", "Houston Texans", "Houston"],
    ["IND", "Indianapolis Colts", "Indianapolis"],
    ["JAX", "Jacksonville Jaguars", "Jacksonville"],
    ["KAN", "Kansas City Chiefs", "Kansas City"],
    ["LVR", "Las Vegas Raiders", "Las Vegas"],
    ["LAC", "Los Angeles Chargers", "Los Angeles"],
    ["LAR", "Los Angeles Rams", "Los Angeles"],
    ["MIA", "Miami Dolphins", "Miami"],
    ["MIN", "Minnesota Vikings", "Minneapolis"],
    ["NWE", "New England Patriots", "Boston"],
    ["NOR", "New Orleans Saints", "New Orleans"],
    ["NYG", "New York Giants", "New York"],
    ["NYJ", "New York Jets", "New York"],
    ["PHI", "Philadelphia Eagles", "Philadelphia"],
    ["PIT", "Pittsburgh Steelers", "Pittsburgh"],
    ["SFO", "San Francisco 49ers", "San Francisco"],
    ["SEA", "Seattle Seahawks", "Seattle"],
    ["TAM", "Tampa Bay Buccaneers", "Tampa"],
    ["TEN", "Tennessee Titans", "Nashville"],
    ["WAS", "Washington Commanders", "Washington"],
];

// ─── Position normalization ────────────────────────────────────────
// Splits raw value on - / , then maps each token to canonical position(s).
// Covers all 751 unique values across the full 25k player dataset.

const TOKEN_MAP = {
    // Quarterback
    QB: ["QB"],

    // Backfield (historical terms)
    RB: ["RB"],
    HB: ["RB"], // halfback
    TB: ["RB"], // tailback
    WB: ["RB"], // wingback
    B: ["RB"], // generic back
    BB: ["FB"], // blocking back
    FB: ["FB"],

    // Receivers (historical terms all map to WR)
    WR: ["WR"],
    FL: ["WR"], // flanker
    SE: ["WR"], // split end
    E: ["WR"], // end (pre-modern receiver)
    PR: ["WR"], // punt returner

    // Tight end
    TE: ["TE"],

    // Offensive line
    OL: ["OT"], // generic OL → OT
    OT: ["OT"],
    T: ["OT"],
    OG: ["OG"],
    G: ["OG"],
    DG: ["OG"], // old two-way guard term
    C: ["C"],

    // Defensive line
    DL: ["DE"], // generic DL → DE
    DE: ["DE"],
    DT: ["DT"],
    NT: ["DT"], // nose tackle → DT
    MG: ["DT"], // middle guard → DT

    // Linebackers
    LB: ["OLB"], // generic LB → OLB
    OLB: ["OLB"],
    ILB: ["MLB"],

    // Secondary
    DB: ["CB"], // generic DB → CB
    CB: ["CB"],
    S: ["SS", "FS"], // ambiguous → store both
    SS: ["SS"],
    FS: ["FS"],

    // Special teams
    K: ["K"],
    P: ["P"],
    LS: ["LS"],

    // Data errors
    Nebraska: [],
};

export const normalizePositions = (raw) => {
    if (!raw) return [];
    const tokens = raw.trim().split(/[-\/,]/);
    const results = [];
    for (const token of tokens) {
        const t = token.trim();
        const mapped = TOKEN_MAP[t];
        if (mapped === undefined) {
            console.warn(`⚠ Unmapped position token: "${t}" (from "${raw}")`);
            continue;
        }
        for (const pos of mapped) {
            if (!results.includes(pos)) results.push(pos);
        }
    }
    return results;
};
