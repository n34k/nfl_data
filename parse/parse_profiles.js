import fs from "fs";

const profiles = JSON.parse(fs.readFileSync("./data/player_profiles.json", "utf8"));

const filtered = profiles.filter((p) => {
    const draftYear = p.draft_year ? parseInt(p.draft_year, 10) : null;
    const birthDate = p.birth_date ?? null;

    // Has a draft year — keep if drafted 1985 or later
    if (draftYear !== null) {
        return draftYear >= 1985;
    }

    // No draft year (undrafted/unknown) — fall back to birth date
    // Born 1963 or later means at most ~40 years old in 2003
    if (birthDate) {
        return birthDate >= "1963-01-01";
    }

    // No draft year and no birth date — drop them, no way to know
    return false;
});

fs.writeFileSync("./data/profiles_filtered.json", JSON.stringify(filtered, null, 2));

console.log(`Original:  ${profiles.length} players`);
console.log(`Filtered:  ${filtered.length} players`);
console.log(`Dropped:   ${profiles.length - filtered.length} players`);
