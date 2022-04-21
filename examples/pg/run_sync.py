import yesql.dynamic
from db import client, model


def run():
    posts = client.SyncPosts()
    posts.initialize()
    post = model.Post(
        title="My Great Blog Post",
        subtitle="It's super great. Trust me...",
        tagline="You'll be glad you read it.",
        tags={"tips", "tricks", "cool stuff"},
    )
    persisted = posts.create(**posts.get_kvs(post))
    print(f"Created a post: {persisted!r}")
    matches = posts.search(words="super & great")
    print(f"Found matches for keywords: {matches!r}")
    matches = posts.search_phrase(phrase="super great")
    print(f"Found matches for phrase: {matches!r}")
    matches = posts.get_by_tags(tags={"tips", "tricks"})
    print(f"Found matches for tags: {matches!r}")
    persisted.title = "Maybe It's Not So Great After All"
    updated = posts.update(id=persisted.id, **posts.get_kvs(persisted))
    print(f"Updated a post: {updated!r}")
    deleted = posts.delete(id=persisted.id)
    print(f"Deleted a post: {deleted!r}")
    posts.bulk_create(params=[posts.get_kvs(post)], returns=False)
    created = posts.all()
    print(f"Bulk-created posts: {created}")
    posts.delete(id=created[0].id)


def dynamic():
    posts = client.SyncPosts()
    posts.initialize()
    dyn = yesql.dynamic.DynamicQueryService(posts, schema="blog")
    post = model.Post(
        title="My Great Blog Post",
        subtitle="It's super great. Trust me...",
        tagline="You'll be glad you read it.",
        tags={"tips", "tricks", "cool stuff"},
    )
    persisted = posts.create(**posts.get_kvs(post))
    found = dyn.select(title=persisted.title)
    print(f"Dynamically selected posts: {found}")
    q = dyn.table.select(dyn.table.id).where(dyn.table.title == persisted.title)
    id = dyn.execute(q, modifier="scalar")
    print(f"Dynamically queried for Post ID with {post.title=}: {id=}")
    posts.delete(id=persisted.id)


if __name__ == "__main__":
    import os

    os.environ[
        "database_url"
    ] = "postgres://postgres:@localhost:5432/blog?sslmode=disable"
    run()
    dynamic()
