# Sri's Book Recommendations – Backend

Simple FastAPI backend with persistent JSON storage, CSV deduplication, conflict handling, and recommendation scoring.

## Setup

```bash
cd backend
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Run the server

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

- API: http://localhost:8000
- Interactive docs: http://localhost:8000/docs

## Storage

Books are persisted to **`books_db.json`** (created automatically next to `main.py`).
The file is loaded into memory on startup and saved to disk after every mutation.

## CSV format

Upload a CSV with these columns (header row required). `book_ID` is **not** required — the backend auto-assigns sequential IDs to new books. If a `book_ID` column is present, it will be used for dedup; otherwise dedup is done by title + author.

| Column | Type | Notes |
|--------|------|-------|
| `book_title` | string | Dedup key (with author) |
| `book_author` | string | Dedup key (with title) |
| `sri_Rating` | float | Your personal rating |
| `goodreads_avg_rating` | float | |
| `goodreads_rating_count` | int | |
| `page_count` | int | Used for length scoring |
| `Genre_Intent` | string | e.g. escapist, mystery-suspense |
| `Pace` | string | fast, moderate, slow |
| `Plot_Character` | string | plot, character, balanced |
| `Mood_Finish` | string | e.g. escapist, emotional |

## API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | API info |
| GET | `/books` | Count of books in the database |
| POST | `/upload-csv` | Upload CSV (dedup + conflict detection) |
| GET | `/conflicts` | View pending conflicts from last upload |
| POST | `/confirm-updates` | Confirm updates for conflicted books |
| POST | `/recommend` | Get ranked recommendations |

---

### POST /upload-csv

Upload a CSV file as form-data (field name: `file`).

**Example response:**

```json
{
  "message": "Processed 5 rows: 2 added, 1 skipped (duplicates), 2 conflicts.",
  "added_books": [
    { "book_ID": "101", "book_title": "Project Hail Mary", "book_author": "Andy Weir" },
    { "book_ID": "102", "book_title": "Atomic Habits", "book_author": "James Clear" }
  ],
  "skipped_books": [
    { "book_ID": "100", "book_title": "The Midnight Library", "book_author": "Matt Haig" }
  ],
  "conflicted_books": [
    {
      "book_ID": "103",
      "book_title": "Educated",
      "book_author": "Tara Westover",
      "differences": {
        "sri_Rating": { "old": "4.0", "new": "4.5" },
        "Pace": { "old": "moderate", "new": "slow" }
      }
    },
    {
      "book_ID": "104",
      "book_title": "Sapiens",
      "book_author": "Yuval Noah Harari",
      "differences": {
        "goodreads_avg_rating": { "old": "4.36", "new": "4.38" }
      }
    }
  ]
}
```

---

### POST /confirm-updates

Send book IDs you want to update after reviewing conflicts.

**Request body:**

```json
{
  "book_ids": ["103", "104"]
}
```

**Example response:**

```json
{
  "message": "Updated 2 books.",
  "updated": ["103", "104"],
  "not_found": [],
  "remaining_conflicts": 0
}
```

---

### GET /conflicts

View all pending conflicts from the last upload (without uploading again).

---

### POST /recommend

**Request body:**

```json
{
  "genre_intent": "escapist",
  "pace": "fast",
  "plot_character": "plot",
  "mood_finish": "emotional",
  "length": "medium"
}
```

**Field mapping from your form:**

| Form field | JSON key | CSV column |
|-----------|----------|------------|
| genre | `genre_intent` | `Genre_Intent` |
| pace | `pace` | `Pace` |
| focus | `plot_character` | `Plot_Character` |
| emotional_weight | `mood_finish` | `Mood_Finish` |
| length | `length` | `page_count` (range) |

Length ranges: short < 200, medium 201–400, long 401–600, epic 600+.

**Response:** `{ "books": [ ... ], "total": N }` — each book has all CSV fields plus `match_score` (0–5).
