from pypika import Query, Table, Field, functions as fn

class CategoryTranslate:

    def __init__(self, db):
        self.db = db

    def getCategories(self):
        categories_shadow = Table('categories_shadow')
        q = Query.from_(categories_shadow).select(
            fn.Count(categories_shadow.id)
        )

        # print(self.db)
        records = self.db.fetchone(q.get_sql())
        # self.db.close()
        return records