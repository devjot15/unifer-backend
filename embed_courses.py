import os, time
from openai import OpenAI
from supabase import create_client

OPENAI_API_KEY  = os.environ.get("OPENAI_API_KEY", "")
SUPABASE_URL    = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY    = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
EMBEDDING_MODEL = "text-embedding-3-small"
BATCH_SIZE      = 100
SLEEP_BETWEEN   = 0.3
PAGE_SIZE       = 1000

openai_client = OpenAI(api_key=OPENAI_API_KEY)
supabase      = create_client(SUPABASE_URL, SUPABASE_KEY)

def embed_texts(texts):
    response = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=texts)
    return [item.embedding for item in response.data]

def embed_new_courses():
    courses = []
    page = 0
    while True:
        result = supabase.schema("core").table("courses").select(
            "id, name, field_category"
        ).is_("embedding", "null").range(
            page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1
        ).execute()
        batch_data = result.data
        if not batch_data:
            break
        courses.extend(batch_data)
        if len(batch_data) < PAGE_SIZE:
            break
        page += 1

    total = len(courses)
    print(f"[embed] {total} courses need embeddings", flush=True)
    if total == 0:
        return 0

    done = 0
    errors = 0
    for i in range(0, total, BATCH_SIZE):
        batch = courses[i : i + BATCH_SIZE]
        texts = [
            f"{c.get('name') or ''} — {c.get('field_category') or ''} — postgraduate university course"
            for c in batch
        ]
        try:
            embeddings = embed_texts(texts)
        except Exception as e:
            print(f"[embed] OpenAI error batch {i//BATCH_SIZE + 1}: {e}", flush=True)
            errors += 1
            time.sleep(2)
            continue
        emb_strings = ["[" + ",".join(str(x) for x in emb) + "]" for emb in embeddings]
        try:
            supabase.rpc("bulk_update_course_embeddings", {
                "p_course_ids": [b["id"] for b in batch],
                "p_embeddings": emb_strings
            }).execute()
        except Exception as e:
            print(f"[embed] Supabase error batch {i//BATCH_SIZE + 1}: {e}", flush=True)
            errors += 1
            continue
        done += len(batch)
        print(f"[embed] [{done}/{total}] {done/total*100:.1f}% done", flush=True)
        time.sleep(SLEEP_BETWEEN)

    print(f"[embed] complete — {done} embedded, {errors} errors", flush=True)
    return done

if __name__ == "__main__":
    embed_new_courses()
