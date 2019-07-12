from pypika import Query, Table, Field

class CategoryTranslate:

    def __init__(self, db):
        self.db = db

    def getCategories(self):
        category_translations = Table('category_translations', 'eg_product')
        q = Query.from_(category_translations).select(
            '*'
        ).where(
            (category_translations.category_id.isin([2, 3, 4, 5, 6])) &
            (category_translations.locale == 'th')
        )

        records = self.db.execute(q)
        return records
        # for row in records:
        #     print(row)