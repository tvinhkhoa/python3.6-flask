from pypika import Query, Table, Tables, Field, functions as fn, Order, JoinType, analytics as an

class EgProduct:

    def __init__(self, db, tail='_shadow'):
        self.db = db
        self.tail = tail
    #1
    def getTotalProductByType(self):
        products = Table('products_shadow', 'eg_product' + self.tail)
        q = Query.from_(products).select(
            fn.Count(products.id), products.type
        ).groupby(
            products.type
        )

        records = self.db.fetchall(q.get_sql())
        return records

    #2
    def getTotalBrand(self):
        manufacturers = Table('manufacturers_shadow', 'eg_product' + self.tail)
        q = Query.from_(manufacturers).select(
            fn.Count(manufacturers.id)
        )

        record = self.db.fetchone(q.get_sql())
        return record

    #3
    def getProductTop10Brand(self):
        products = Table('products_shadow', 'eg_product' + self.tail)
        manufacturers = Table('manufacturers_shadow', 'eg_product' + self.tail)
        q = Query.from_(products).join(
            manufacturers
        ).on(
            products.manufacturer_id == manufacturers.id
        ).select(
            fn.Count('*'), manufacturers.url_rewrite
        ).where(
            (products.is_main == 1) &
            (manufacturers.url_rewrite.isin([854, 177, 704, 345]))
        ).groupby(
            manufacturers.url_rewrite
        ).orderby(
            manufacturers.url_rewrite, order=Order.asc
        )

        records = self.db.fetchall(q.get_sql())
        return records

    #4
    def getTotalMoto(self):
        motors = Table('motors_shadow', 'eg_product' + self.tail)
        q = Query.from_(motors).select(
            fn.Count('*')
        )

        record = self.db.fetchone(q.get_sql())
        return record

    #5
    def getProductTop10Moto(self):
        product_motor = Table('product_motor_shadow', 'eg_product' + self.tail)
        products = Table('products_shadow', 'eg_product' + self.tail)
        motors = Table('motors_shadow', 'eg_product' + self.tail)
        q = Query.from_(product_motor).join(
            products
        ).on(
            products.id == product_motor.product_id
        ).join(
            motors
        ).on(
            product_motor.motor_id == motors.id
        ).select(
            fn.Count('*'), motors.url_rewrite
        ).where(
            (products.is_main == 1) &
            (motors.url_rewrite.isin([40, 874, 163, 821, 825, 462, 463]))
        ).groupby(
            product_motor.motor_id
        )

        records = self.db.fetchall(q.get_sql())
        return records

    #6
    def getTotalCategories(self):
        categories = Table('categories_shadow', 'eg_product' + self.tail)
        q = Query.from_(categories).select(
            fn.Count('*')
        )

        record = self.db.fetchone(q.get_sql())
        return record

    #7
    def getTotalProductTop10Cate(self):
        category_product = Table('category_product_shadow', 'eg_product' + self.tail)
        categories = Table('categories_shadow', 'eg_product' + self.tail)
        products = Table('products_shadow', 'eg_product' + self.tail)
        q = Query.from_(category_product).join(
            categories
        ).on(
            categories.id == category_product.category_id
        ).join(
            products
        ).on(
            products.id == category_product.product_id
        ).select(
            fn.Count('*'), categories.url_rewrite
        ).where(
            (products.is_main == 1) &
            (categories.url_rewrite.isin([3002, 3021, 3123, 3034, 5000]))
        ).groupby(
            categories.url_rewrite
        )

        records = self.db.fetchall(q.get_sql())
        return records

    #8
    def getModelIncorrect(self):
        product_motor = Table('product_motor_shadow', 'eg_product' + self.tail)
        products = Table('products_shadow', 'eg_product' + self.tail)
        motors = Table('motors_shadow', 'eg_product' + self.tail)
        sys_code = Table('syouhin_sys_code', 'eg_product' + self.tail)
        mst_syouhin_model_info_all = Table('mst_syouhin_model_info_all', 'rc_products')

        qSub1 = Query.from_(products).join(
            product_motor, how=JoinType.inner
        ).on(
            product_motor.product_id == products.id
        ).join(
            motors, how=JoinType.inner
        ).on(
            motors.id == product_motor.motor_id
        ).select(
            products.sku, "GROUP_CONCAT(motors_shadow.url_rewrite ORDER BY CAST(motors_shadow.url_rewrite AS UNSIGNED)) as model"
        ).groupby(
            products.sku
        ).limit(100000)

        qSub2 = Query.from_()
        # SELECT
        #     syouhin_sys_code
        #     , GROUP_CONCAT( 
        #         mst_syouhin_model_info_all.syouhin_model_code 
        #         order by
        #         CAST(syouhin_model_code AS UNSIGNED) SEPARATOR '-'
        #     ) as model
        #     , count(syouhin_model_code) as model_count 
        #     FROM
        #     rc_products.mst_syouhin_model_info_all 
        #         WHERE mst_syouhin_model_info_all.syouhin_model_code != 9999
        #     group by
        #     syouhin_sys_code 
        #     limit
        #     0, 100000

        

        finds = ['"', 'products_shadow.GROUP_CONCAT']
        replaces = ['', 'GROUP_CONCAT']
        qSub1 = qSub1.get_sql()
        for find, replace in zip(finds, replaces):
            qSub1 = qSub1.replace(find, replace)

        records = self.db.fetchall(qSub1)
        return records
        # select
            
        #     , GROUP_CONCAT( 
        #         motors_shadow.url_rewrite 
        #         ORDER BY
        #         CAST(motors_shadow.url_rewrite AS UNSIGNED) SEPARATOR '-'
        #     ) as model
        #     , count(motors_shadow.url_rewrite) as model_count 
        # from
        #     eg_product_shadow.products_shadow 
        # inner join eg_product_shadow.product_motor_shadow 
        #     on product_motor_shadow.product_id = products_shadow.id 
        # inner join eg_product_shadow.motors_shadow 
        #     on  
        # group by
            
        # limit
        #     0, 100000
