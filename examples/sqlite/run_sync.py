import norma.dynamic
from db import client, model


def run():
    posts = client.SyncPosts()
    post = model.Post(
        title="My Great Blog Post",
        subtitle="It's super great. Trust me...",
        tagline="You'll be glad you read it.",
    )
    persisted = posts.create(model=post)
    print(f"Created a post: {persisted!r}")
    fetched = posts.get(id=persisted.id)
    print(f"Fetched a post: {fetched!r}")
    persisted.title = "Maybe It's Not So Great After All"
    updated = posts.update(model=persisted, id=persisted.id)
    print(f"Updated a post: {updated!r}")
    deleted = posts.delete(id=persisted.id)
    print(f"Deleted a post: {deleted!r}")


def dynamic():
    posts = client.SyncPosts()
    dyn = norma.dynamic.SyncDynamicQueryLib(posts)
    post = model.Post(
        title="My Great Blog Post",
        subtitle="It's super great. Trust me...",
        tagline="You'll be glad you read it.",
    )
    persisted = posts.create(model=post)
    found = dyn.select(title=persisted.title)
    print(f"Dynamically selected posts: {found}")
    q = dyn.table.select(dyn.table.id).where(dyn.table.title == persisted.title)
    id = dyn.execute(q, rtype="val")
    print(f"Dynamically for Post ID with {post.title=}: {id=}")
    posts.delete(id=persisted.id)


if __name__ == "__main__":
    import os

    os.environ["database_url"] = "blog.db"
    run()
    dynamic()
