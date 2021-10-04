from db import client, model


def run():
    posts = client.SyncPosts()
    post = model.Post(
        title="My Great Blog Post",
        subtitle="It's super great. Trust me...",
        tagline="You'll be glad you read it.",
        tags={"tips", "tricks", "cool stuff"},
    )
    persisted = posts.create(model=post)
    print(f"Created a post: {persisted!r}")
    matches = posts.search(words="super & great")
    print(f"Found matches for keywords: {matches!r}")
    matches = posts.search_phrase(phrase="super great")
    print(f"Found matches for phrase: {matches!r}")
    matches = posts.get_by_tags(tags={"tips", "tricks"})
    print(f"Found matches for tags: {matches!r}")
    persisted.title = "Maybe It's Not So Great After All"
    updated = posts.update(model=persisted, id=persisted.id)
    print(f"Updated a post: {updated!r}")
    deleted = posts.delete(id=persisted.id)
    print(f"Deleted a post: {deleted!r}")


if __name__ == "__main__":
    import os

    os.environ[
        "database_url"
    ] = "postgres://postgres:@localhost:5432/blog?sslmode=disable"
    run()
