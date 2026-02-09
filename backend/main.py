"""
Sri's Book Recommendations – Backend API

Features:
- Persistent JSON-based book database (file-backed, loaded into memory on start).
- CSV upload with deduplication and conflict detection.
- Confirm-update flow for conflicted books.
- Recommendation scoring based on user preferences.
"""

import csv
import io
import json
import math
import os
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx
from fastapi import FastAPI, File, Header, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# =============================================================================
# Configuration
# =============================================================================

# Path to the persistent JSON file that stores all books.
# Sits next to this script so it's easy to find / back up.
DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "books_db.json")

# Admin key for protecting write endpoints (edit, delete, upload).
# Change this to your own secret. You can also set the ADMIN_KEY env variable.
ADMIN_KEY = os.environ.get("ADMIN_KEY", "sri2026books")

# Columns expected in the uploaded CSV (book_ID is NOT required; auto-generated)
CSV_COLUMNS = [
    "book_title",
    "book_author",
    "sri_Rating",
    "goodreads_avg_rating",
    "goodreads_rating_count",
    "page_count",
    "Genre_Intent",
    "Pace",
    "Plot_Character",
    "Mood_Finish",
]

# All columns stored in the database (CSV columns + ID + derived title fields)
# - book_ID           : auto-generated sequential integer
# - goodreads_title   : used for display (defaults to book_title on import)
# - cover_search_title: used to search for cover images (Open Library / Google Books)
DB_COLUMNS = ["book_ID"] + CSV_COLUMNS + ["goodreads_title", "cover_search_title", "cover_image_url"]

# Genre_Intent is used as a FILTER (not scored).
# These remaining fields are used for recommendation SCORING.
SCORING_FIELDS = ["Pace", "Plot_Character", "Mood_Finish"]

# Maximum possible score when all fields have a specific preference (not "any")
# = 3 text fields + 1 length match = 4
# Actual max_score per request is dynamic: fields set to "any" are excluded.
MAX_SCORE = 4

# =============================================================================
# In-memory book database
# =============================================================================

# Primary store: dict keyed by book_ID for fast lookup.
# Each value is a dict with all CSV_COLUMNS as keys.
books_db: Dict[str, Dict[str, Any]] = {}

# Temporary store for conflicts from the most recent upload.
# Keyed by book_ID → {"old": {...}, "new": {...}}
# Cleared on every new upload.
pending_conflicts: Dict[str, Dict[str, Any]] = {}


# =============================================================================
# Persistence helpers
# =============================================================================


def load_db() -> None:
    """
    Load books from the JSON file on disk into the in-memory dict.
    Called once when the server starts.
    If the file doesn't exist yet, start with an empty database.
    """
    global books_db
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # data is a list of book dicts → convert to dict keyed by book_ID
        books_db = {}
        migrated = False
        for book in data:
            # Migrate: if older records don't have the title fields, add them
            if "goodreads_title" not in book:
                book["goodreads_title"] = book.get("book_title", "")
                migrated = True
            if "cover_search_title" not in book:
                book["cover_search_title"] = book.get("googlebooks_title", "") or book.get("book_title", "")
                migrated = True
            if "cover_image_url" not in book:
                book["cover_image_url"] = ""
                migrated = True
            # Remove old field name if present
            book.pop("googlebooks_title", None)
            key = _book_key(book)
            books_db[key] = book
        print(f"[DB] Loaded {len(books_db)} books from {DB_FILE}")
        if migrated:
            save_db()
            print("[DB] Migrated existing books to include goodreads_title & cover_search_title.")
    else:
        books_db = {}
        print(f"[DB] No existing database found. Starting fresh.")


def save_db() -> None:
    """
    Persist the current in-memory book dict back to disk as a JSON array.
    Called after any mutation (add / update).
    """
    data = list(books_db.values())
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"[DB] Saved {len(data)} books to {DB_FILE}")


def _next_book_id() -> str:
    """
    Generate the next sequential book_ID.
    Scans existing IDs, finds the max numeric one, and returns max + 1 as a string.
    """
    max_id = 0
    for book in books_db.values():
        try:
            bid = int(book.get("book_ID", 0))
            if bid > max_id:
                max_id = bid
        except (TypeError, ValueError):
            pass
    return str(max_id + 1)


def _book_key(book: Dict[str, Any]) -> str:
    """
    Generate a unique lookup key for a book.
    Primary:  book_ID  (if present and non-empty)
    Fallback: book_title + "|" + book_author  (lowercased, stripped)
    """
    bid = str(book.get("book_ID", "")).strip()
    if bid:
        return bid
    # Fallback: title + author combo
    title = str(book.get("book_title", "")).strip().lower()
    author = str(book.get("book_author", "")).strip().lower()
    return f"{title}|{author}"


# =============================================================================
# FastAPI app
# =============================================================================

app = FastAPI(
    title="Sri's Book Recommendations API",
    description="Upload book CSVs (with dedup + conflict handling) and get personalized recommendations.",
)

# Allow the frontend (served from a different origin) to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    """Load the persistent book database when the server starts."""
    load_db()


@app.get("/health")
def health():
    """Lightweight health-check endpoint (used by keep-alive cron)."""
    return {"status": "ok"}


# =============================================================================
# Helpers
# =============================================================================


def require_admin(x_admin_key: Optional[str]) -> None:
    """
    Check the X-Admin-Key header against the configured ADMIN_KEY.
    Raises 403 if missing or wrong. Used on all write endpoints.
    """
    if not x_admin_key or x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Forbidden. Invalid admin key.")


def normalize(value: Optional[str]) -> str:
    """Normalize a value for comparison: strip whitespace, lowercase."""
    if value is None:
        return ""
    return str(value).strip().lower()


def parse_book_row(row: Dict[str, str]) -> Dict[str, Any]:
    """
    Take a raw CSV row dict and return a clean book dict
    with only the expected columns and proper types.
    book_ID is NOT expected from CSV — it will be assigned separately.
    On initial import, both goodreads_title and cover_search_title
    are set to book_title (the CSV value). They can be edited
    independently later via the admin panel.
    """
    book: Dict[str, Any] = {}
    for col in CSV_COLUMNS:
        book[col] = str(row.get(col, "")).strip()
    # If the CSV happens to include book_ID (legacy format), keep it
    if "book_ID" in row and str(row["book_ID"]).strip():
        book["book_ID"] = str(row["book_ID"]).strip()
    else:
        book["book_ID"] = ""  # Will be assigned by the upload endpoint
    # Convert numeric fields for easier use later
    for num_field in ["sri_Rating", "goodreads_avg_rating"]:
        try:
            book[num_field] = float(book[num_field]) if book[num_field] else 0.0
        except (TypeError, ValueError):
            book[num_field] = 0.0
    for int_field in ["goodreads_rating_count", "page_count"]:
        try:
            book[int_field] = int(book[int_field]) if book[int_field] else 0
        except (TypeError, ValueError):
            book[int_field] = 0
    # Set both display and image-fetch titles to the CSV title initially
    title = book["book_title"]
    book["goodreads_title"] = str(row.get("goodreads_title", "")).strip() or title
    book["cover_search_title"] = str(row.get("cover_search_title", "")).strip() or title
    book["cover_image_url"] = ""  # Will be resolved after insertion
    return book


def books_are_equal(old: Dict[str, Any], new: Dict[str, Any]) -> bool:
    """
    Compare two book dicts field-by-field for all DB_COLUMNS.
    Returns True if every field is identical (after converting to string for safety).
    """
    for col in DB_COLUMNS:
        if str(old.get(col, "")) != str(new.get(col, "")):
            return False
    return True


def diff_fields(old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """
    Return a dict of fields that differ between old and new.
    Format:  { "field_name": { "old": "...", "new": "..." }, ... }
    """
    diffs: Dict[str, Dict[str, str]] = {}
    for col in DB_COLUMNS:
        old_val = str(old.get(col, ""))
        new_val = str(new.get(col, ""))
        if old_val != new_val:
            diffs[col] = {"old": old_val, "new": new_val}
    return diffs


# =============================================================================
# Cover image resolution
# =============================================================================

# Shared HTTP client – reused across requests for connection pooling.
_http_client: Optional[httpx.Client] = None


def _get_http_client() -> httpx.Client:
    """Lazily create a reusable httpx client."""
    global _http_client
    if _http_client is None:
        _http_client = httpx.Client(timeout=10.0, follow_redirects=True)
    return _http_client


def _cover_from_open_library(title: str, author: str) -> Optional[str]:
    """Search Open Library for a cover image. Returns URL or None."""
    query = f"{title} {author}".strip() if author else title.strip()
    if not query:
        return None
    url = f"https://openlibrary.org/search.json?q={quote(query)}&limit=1&fields=cover_i"
    try:
        resp = _get_http_client().get(url)
        if resp.status_code == 200:
            data = resp.json()
            docs = data.get("docs", [])
            if docs and docs[0].get("cover_i"):
                cover_id = docs[0]["cover_i"]
                return f"https://covers.openlibrary.org/b/id/{cover_id}-M.jpg"
    except Exception:
        pass
    return None


def _cover_from_google_books(title: str, author: str) -> Optional[str]:
    """Search Google Books for a cover image. Returns URL or None."""
    query = f"{title} {author}".strip() if author else title.strip()
    if not query:
        return None
    url = f"https://www.googleapis.com/books/v1/volumes?q={quote(query)}&maxResults=1"
    try:
        resp = _get_http_client().get(url)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("items", [])
            if items:
                image_links = items[0].get("volumeInfo", {}).get("imageLinks", {})
                thumb = image_links.get("thumbnail") or image_links.get("smallThumbnail")
                if thumb:
                    # Google returns http URLs; upgrade to https
                    return thumb.replace("http://", "https://")
    except Exception:
        pass
    return None


def resolve_cover_url(title: str, author: str) -> str:
    """
    Resolve a book cover image URL using the same 3-step fallback as the frontend:
    1. Open Library (title + author)
    2. Google Books (title + author)
    3. Open Library (title only, no author)
    Returns the image URL or empty string if none found.
    """
    url = _cover_from_open_library(title, author)
    if url:
        return url
    url = _cover_from_google_books(title, author)
    if url:
        return url
    url = _cover_from_open_library(title, "")
    if url:
        return url
    return ""


# =============================================================================
# Request / Response models
# =============================================================================


class RecommendRequest(BaseModel):
    """Form data sent by the frontend when user clicks 'Get Recommendation'."""
    genre_intent: str   # → Genre_Intent
    pace: str           # → Pace
    plot_character: str  # → Plot_Character
    mood_finish: str    # → Mood_Finish
    length: str         # → page_count range (short/medium/long/epic)


class ConfirmUpdatesRequest(BaseModel):
    """List of book_IDs the user confirms for update after a conflicted upload."""
    book_ids: List[str]


class BookUpdateRequest(BaseModel):
    """
    Partial update for a single book.
    Every field is optional – only send the fields you want to change.
    """
    book_title: Optional[str] = None
    book_author: Optional[str] = None
    goodreads_title: Optional[str] = None
    cover_search_title: Optional[str] = None
    sri_Rating: Optional[float] = None
    goodreads_avg_rating: Optional[float] = None
    goodreads_rating_count: Optional[int] = None
    page_count: Optional[int] = None
    Genre_Intent: Optional[str] = None
    Pace: Optional[str] = None
    Plot_Character: Optional[str] = None
    Mood_Finish: Optional[str] = None


# =============================================================================
# Endpoints
# =============================================================================


@app.get("/")
def root():
    """Health check / API info."""
    return {
        "message": "Sri's Book Recommendations API",
        "endpoints": {
            "GET    /books": "Number of books in the database",
            "GET    /books/all": "List all books with full details",
            "PUT    /books/{book_id}": "Update fields of a single book",
            "DELETE /books/{book_id}": "Delete a single book",
            "POST   /upload-csv": "Upload a CSV (dedup + conflict detection)",
            "GET    /conflicts": "View pending conflicts from last upload",
            "POST   /confirm-updates": "Confirm updates for conflicted books",
            "POST   /resolve-covers": "Resolve/refresh cover image URLs for all books",
            "POST   /recommend": "Get ranked recommendations (JSON body)",
        },
    }


@app.get("/books")
def get_books_info():
    """Return how many books are currently in the persistent database."""
    return {"count": len(books_db), "loaded": len(books_db) > 0}


@app.get("/books/all")
def get_all_books(offset: int = 0, limit: int = 0):
    """
    Return books sorted by sri_Rating desc, then Goodreads popularity desc.
    Supports pagination: ?offset=0&limit=10  (limit=0 means all).
    """
    def sort_key(book: Dict[str, Any]):
        sri = book.get("sri_Rating", 0.0)
        if isinstance(sri, str):
            try:
                sri = float(sri)
            except (TypeError, ValueError):
                sri = 0.0
        gr_r = book.get("goodreads_avg_rating", 0.0)
        if isinstance(gr_r, str):
            try:
                gr_r = float(gr_r)
            except (TypeError, ValueError):
                gr_r = 0.0
        gr_v = book.get("goodreads_rating_count", 0)
        if isinstance(gr_v, str):
            try:
                gr_v = int(gr_v)
            except (TypeError, ValueError):
                gr_v = 0
        gr_pop = gr_r * math.log(1 + gr_v)
        return (-sri, -gr_pop)

    sorted_books = sorted(books_db.values(), key=sort_key)
    total = len(sorted_books)

    if limit > 0:
        page = sorted_books[offset:offset + limit]
    else:
        page = sorted_books[offset:] if offset > 0 else sorted_books

    return {"books": page, "count": total, "offset": offset, "limit": limit}


@app.put("/books/{book_id}")
def update_book(book_id: str, body: BookUpdateRequest, x_admin_key: Optional[str] = Header(None)):
    """
    Update individual fields of a single book identified by book_ID.
    Requires X-Admin-Key header.
    """
    require_admin(x_admin_key)
    if book_id not in books_db:
        raise HTTPException(status_code=404, detail=f"Book '{book_id}' not found.")

    book = books_db[book_id]
    # Apply only the fields that were explicitly sent (not None)
    changes = body.dict(exclude_none=True)
    if not changes:
        raise HTTPException(status_code=400, detail="No fields to update.")

    old_values = {}
    for field, new_val in changes.items():
        old_values[field] = book.get(field)
        book[field] = new_val

    # If book_ID itself hasn't changed, the key stays the same.
    # If title or author changed (fallback key), re-key if needed.
    new_key = _book_key(book)
    if new_key != book_id:
        books_db[new_key] = book
        del books_db[book_id]

    save_db()

    return {
        "message": f"Book '{book.get('book_title', book_id)}' updated.",
        "book": book,
        "changed_fields": {k: {"old": old_values[k], "new": v} for k, v in changes.items()},
    }


@app.delete("/books/{book_id}")
def delete_book(book_id: str, x_admin_key: Optional[str] = Header(None)):
    """Delete a single book from the database. Requires X-Admin-Key header."""
    require_admin(x_admin_key)
    if book_id not in books_db:
        raise HTTPException(status_code=404, detail=f"Book '{book_id}' not found.")

    removed = books_db.pop(book_id)
    save_db()

    return {
        "message": f"Book '{removed.get('book_title', book_id)}' deleted.",
        "book_ID": book_id,
    }


# ---- CSV Upload with dedup + conflict detection ----------------------------


@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...), x_admin_key: Optional[str] = Header(None)):
    """
    Upload a CSV file. Requires X-Admin-Key header. For each row:
      1. If the book is NEW (not in the database)       → add it.
      2. If the book EXISTS and all fields match exactly → skip it (duplicate).
      3. If the book EXISTS but some fields differ       → mark as conflict.

    Returns a structured response with added, skipped, and conflicted books.
    Conflicts are stored in memory so the user can confirm updates via
    POST /confirm-updates.
    """
    require_admin(x_admin_key)
    global pending_conflicts

    # --- Validate file -------------------------------------------------------
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file.")

    contents = await file.read()
    try:
        text = contents.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File must be UTF-8 encoded.")

    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)

    if not rows:
        raise HTTPException(status_code=400, detail="CSV file is empty.")

    # Check required columns exist
    first = rows[0]
    missing = [c for c in CSV_COLUMNS if c not in first]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"CSV missing required columns: {missing}. Expected: {CSV_COLUMNS}",
        )

    # --- Process each row ----------------------------------------------------
    added_books: List[Dict[str, Any]] = []
    skipped_books: List[Dict[str, Any]] = []
    conflicted_books: List[Dict[str, Any]] = []

    # Clear previous conflicts
    pending_conflicts = {}

    for row in rows:
        new_book = parse_book_row(row)

        # --- Find an existing match ------------------------------------------
        # If CSV provides a book_ID, check by ID first.
        # Otherwise (or if ID not found), check by title+author combo.
        key = None
        csv_has_id = bool(new_book.get("book_ID"))

        if csv_has_id and new_book["book_ID"] in books_db:
            key = new_book["book_ID"]
        else:
            # Search by title+author among existing books
            ta_key = (
                str(new_book.get("book_title", "")).strip().lower()
                + "|"
                + str(new_book.get("book_author", "")).strip().lower()
            )
            for existing_key, existing_book in books_db.items():
                existing_ta = (
                    str(existing_book.get("book_title", "")).strip().lower()
                    + "|"
                    + str(existing_book.get("book_author", "")).strip().lower()
                )
                if ta_key == existing_ta:
                    key = existing_key
                    break

        if key is None:
            # ----- Case 1: New book → auto-assign ID and add -----------------
            if not new_book.get("book_ID"):
                new_book["book_ID"] = _next_book_id()
            db_key = _book_key(new_book)
            books_db[db_key] = new_book
            added_books.append({
                "book_ID": new_book["book_ID"],
                "book_title": new_book["book_title"],
                "book_author": new_book["book_author"],
            })

        elif books_are_equal(books_db[key], new_book):
            # ----- Case 2: Exact duplicate → skip silently -------------------
            skipped_books.append({
                "book_ID": books_db[key].get("book_ID", key),
                "book_title": new_book["book_title"],
                "book_author": new_book["book_author"],
            })

        else:
            # ----- Case 3: Exists but fields differ → conflict ---------------
            # Carry over the existing book_ID so the conflict references the right record
            new_book["book_ID"] = books_db[key].get("book_ID", key)
            diffs = diff_fields(books_db[key], new_book)
            pending_conflicts[key] = {
                "old": books_db[key],
                "new": new_book,
            }
            conflicted_books.append({
                "book_ID": new_book["book_ID"],
                "book_title": new_book["book_title"],
                "book_author": new_book["book_author"],
                "differences": diffs,
            })

    # Persist newly added books to disk
    if added_books:
        save_db()

    # Resolve cover images for all newly added books
    covers_resolved = 0
    if added_books:
        print(f"[Covers] Resolving cover images for {len(added_books)} new books...")
        for info in added_books:
            bid = info["book_ID"]
            if bid in books_db:
                book = books_db[bid]
                search_title = book.get("cover_search_title") or book.get("book_title", "")
                author = book.get("book_author", "")
                cover_url = resolve_cover_url(search_title, author)
                book["cover_image_url"] = cover_url
                if cover_url:
                    covers_resolved += 1
                    print(f"  [OK] {book.get('book_title', bid)}")
                else:
                    print(f"  [--] {book.get('book_title', bid)} (no cover found)")
        save_db()
        print(f"[Covers] Done. {covers_resolved}/{len(added_books)} covers found.")

    return {
        "message": (
            f"Processed {len(rows)} rows: "
            f"{len(added_books)} added, "
            f"{len(skipped_books)} skipped (duplicates), "
            f"{len(conflicted_books)} conflicts. "
            f"Covers resolved: {covers_resolved}/{len(added_books)}."
        ),
        "added_books": added_books,
        "skipped_books": skipped_books,
        "conflicted_books": conflicted_books,
    }


# ---- Confirm updates for conflicted books ----------------------------------


@app.post("/confirm-updates")
def confirm_updates(body: ConfirmUpdatesRequest, x_admin_key: Optional[str] = Header(None)):
    """
    Accept a list of book_IDs that the user wants to update.
    Requires X-Admin-Key header.
    """
    require_admin(x_admin_key)
    if not pending_conflicts:
        raise HTTPException(
            status_code=400,
            detail="No pending conflicts. Upload a CSV first.",
        )

    updated: List[str] = []
    not_found: List[str] = []

    for bid in body.book_ids:
        bid_stripped = bid.strip()
        if bid_stripped in pending_conflicts:
            # Apply the new version
            new_book = pending_conflicts[bid_stripped]["new"]
            books_db[bid_stripped] = new_book
            updated.append(bid_stripped)
            # Remove from pending once applied
            del pending_conflicts[bid_stripped]
        else:
            not_found.append(bid_stripped)

    # Persist changes to disk
    if updated:
        save_db()

    return {
        "message": f"Updated {len(updated)} books.",
        "updated": updated,
        "not_found": not_found,
        "remaining_conflicts": len(pending_conflicts),
    }


# ---- Get pending conflicts (for frontend display) --------------------------


@app.get("/conflicts")
def get_conflicts():
    """Return all pending conflicts from the last CSV upload."""
    result = []
    for key, conflict in pending_conflicts.items():
        diffs = diff_fields(conflict["old"], conflict["new"])
        result.append({
            "book_ID": conflict["new"].get("book_ID", key),
            "book_title": conflict["new"].get("book_title", ""),
            "book_author": conflict["new"].get("book_author", ""),
            "differences": diffs,
        })
    return {"conflicts": result, "count": len(result)}


# ---- Resolve / refresh cover images ----------------------------------------


@app.post("/resolve-covers")
def resolve_covers(
    force: bool = False,
    x_admin_key: Optional[str] = Header(None),
):
    """
    Resolve cover image URLs for books in the database.
    By default, only resolves books with an empty cover_image_url.
    Pass ?force=true to re-resolve ALL books (useful if covers have changed).
    Requires X-Admin-Key header.
    """
    require_admin(x_admin_key)
    if not books_db:
        raise HTTPException(status_code=400, detail="No books in the database.")

    resolved = 0
    failed = 0
    skipped = 0

    for book in books_db.values():
        existing = book.get("cover_image_url", "")
        if existing and not force:
            skipped += 1
            continue
        search_title = book.get("cover_search_title") or book.get("book_title", "")
        author = book.get("book_author", "")
        cover_url = resolve_cover_url(search_title, author)
        book["cover_image_url"] = cover_url
        if cover_url:
            resolved += 1
            print(f"  [OK] {book.get('book_title', '?')}")
        else:
            failed += 1
            print(f"  [--] {book.get('book_title', '?')} (no cover found)")

    save_db()

    return {
        "message": f"Cover resolution complete. {resolved} found, {failed} not found, {skipped} skipped (already had cover).",
        "resolved": resolved,
        "failed": failed,
        "skipped": skipped,
        "total": len(books_db),
    }


# ---- Recommendation --------------------------------------------------------


@app.post("/recommend")
def recommend(body: RecommendRequest):
    """
    Filter books by Genre_Intent, then score and rank the remaining books.

    Filter: Genre_Intent must match the user's selection.
    Scoring (max 4):
    - +1 for each matching field: Pace, Plot_Character, Mood_Finish
    - +1 if page_count falls within the user's chosen length range
    Fields set to "any" are excluded from scoring.
    Tie-breakers: sri_Rating desc → Goodreads popularity desc.
    """
    if not books_db:
        raise HTTPException(
            status_code=400,
            detail="No books in the database. Upload a CSV first via POST /upload-csv.",
        )

    # Map request fields → CSV column names (Genre_Intent is a filter, not scored)
    user_values = {
        "Pace": body.pace,
        "Plot_Character": body.plot_character,
        "Mood_Finish": body.mood_finish,
    }

    # --- Step 1: Filter by Genre_Intent --------------------------------------
    genre_filter = normalize(body.genre_intent)
    filtered_books = [
        book for book in books_db.values()
        if normalize(book.get("Genre_Intent")) == genre_filter
    ]

    # Page-count ranges for the length preference
    def page_range_for_length(length_key: str) -> Tuple[int, int]:
        key = normalize(length_key)
        if key == "short":
            return (0, 200)
        if key == "medium":
            return (201, 400)
        if key == "long":
            return (401, 600)
        if key == "epic":
            return (601, 100_000)
        return (0, 0)

    length_key = body.length
    length_low, length_high = page_range_for_length(length_key)

    # Determine which fields are actually being scored (exclude "any")
    active_fields = [
        f for f in SCORING_FIELDS if normalize(user_values.get(f)) != "any"
    ]
    length_active = normalize(length_key) != "any"
    max_score = len(active_fields) + (1 if length_active else 0)

    # --- Step 2: Score filtered books ----------------------------------------
    scored: List[Tuple[Dict[str, Any], int, float, float]] = []
    for book in filtered_books:
        score = 0
        for field in active_fields:
            if normalize(book.get(field)) == normalize(user_values.get(field)):
                score += 1
        # Length: only score if user specified a preference
        if length_active:
            try:
                pages = int(book.get("page_count") or 0)
                if length_low <= pages <= length_high:
                    score += 1
            except (TypeError, ValueError):
                pass
        sri_rating = book.get("sri_Rating", 0.0)
        if isinstance(sri_rating, str):
            try:
                sri_rating = float(sri_rating)
            except (TypeError, ValueError):
                sri_rating = 0.0

        # Second tiebreaker: Goodreads popularity = R * log(1 + v)
        gr_rating = book.get("goodreads_avg_rating", 0.0)
        if isinstance(gr_rating, str):
            try:
                gr_rating = float(gr_rating)
            except (TypeError, ValueError):
                gr_rating = 0.0
        gr_count = book.get("goodreads_rating_count", 0)
        if isinstance(gr_count, str):
            try:
                gr_count = int(gr_count)
            except (TypeError, ValueError):
                gr_count = 0
        gr_popularity = gr_rating * math.log(1 + gr_count)

        scored.append((book, score, float(sri_rating), gr_popularity))

    # Sort: primary = score desc, secondary = sri_Rating desc, tertiary = GR popularity desc
    scored.sort(key=lambda x: (-x[1], -x[2], -x[3]))

    # Build response list
    result = []
    for book, score, _, _ in scored:
        out = dict(book)
        out["match_score"] = score
        out["max_score"] = max_score
        result.append(out)

    return {
        "books": result,
        "total": len(result),
        "max_score": max_score,
        "genre_filter": genre_filter,
        "filtered_from": len(books_db),
    }
