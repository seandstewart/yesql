import factory

from examples.pg.db import model


class PostFactory(factory.Factory):
    class Meta:
        model = model.Post

    title = factory.Faker("catch_phrase")
    subtitle = factory.Faker("bs")
    tagline = factory.Faker("bs")
    body = factory.Faker("paragraph")
