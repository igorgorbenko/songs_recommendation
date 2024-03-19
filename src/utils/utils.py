

class Utils:
    @classmethod
    def get_vector(cls, collection, expr):
        entities = collection.query(expr=expr, output_fields=["embedding"])
        return entities[0]["embedding"] if entities else None
