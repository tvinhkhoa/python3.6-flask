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
            products.sku,
            "GROUP_CONCAT(motors_shadow.url_rewrite ORDER BY CAST(motors_shadow.url_rewrite AS UNSIGNED) SEPARATOR '-') as model",
            fn.Count(motors.url_rewrite).as_('model_count')
        ).groupby(
            products.sku
        ).limit(100000)

        qSub2 = Query.from_(mst_syouhin_model_info_all).select(
            mst_syouhin_model_info_all.syouhin_sys_code,
            "GROUP_CONCAT(mst_syouhin_model_info_all.syouhin_model_code ORDER BY CAST(syouhin_model_code AS UNSIGNED) SEPARATOR '-') as model",
            fn.Count(mst_syouhin_model_info_all.syouhin_model_code).as_('model_count')
        ).where(
            mst_syouhin_model_info_all.syouhin_model_code != 9999
        ).groupby(
            mst_syouhin_model_info_all.syouhin_sys_code
        ).limit(100000)

        finds = ['"', 'products_shadow.GROUP_CONCAT']
        replaces = ['', 'GROUP_CONCAT']

        qSub1 = qSub1.get_sql()
        qSub2 = qSub2.get_sql()
        for find, replace in zip(finds, replaces):
            qSub1 = qSub1.replace(find, replace)
            qSub2 = qSub2.replace(find, replace)

        q = "SELECT real_tbl.sku, real_tbl.model as current_model, real_tbl.model_count, org_tbl.model as correct_model, org_tbl.model_count " + \
            "FROM ({}) as real_tbl".format(qSub1) + " " + \
            "INNER JOIN ({}) as org_tbl".format(qSub2) + " ON real_tbl.sku = org_tbl.syouhin_sys_code " + \
            "WHERE real_tbl.model <> org_tbl.model AND real_tbl.model_count <> org_tbl.model_count"

        records = self.db.fetchall(q)
        return records

    #9
    def getBrandincorrect(self):
        mst_rc_syouhin = Table('mst_rc_syouhin', 'rc_products')
        products = Table('products_shadow', 'eg_product' + self.tail)
        manufacturers = Table('manufacturers_shadow', 'eg_product' + self.tail)
        mst_brand = Table('mst_brand', 'rc_products')

        q = Query.from_(mst_rc_syouhin).join(
            products, how=JoinType.inner
        ).on(
            mst_rc_syouhin.syouhin_sys_code == products.sku
        ).join(
            manufacturers, how=JoinType.left
        ).on(
            manufacturers.id == products.manufacturer_id
        ).join(
            mst_brand, how=JoinType.left
        ).on(
            mst_brand.brand_code == mst_rc_syouhin.brand_code
        ).select(
            mst_rc_syouhin.syouhin_sys_code,
            mst_brand.name.as_('global_brand'),
            mst_rc_syouhin.brand_code.as_('global_brand'),
            manufacturers.url_rewrite.as_('glocal_brand'),
            manufacturers.name.as_('glocal_brand')
        ).where(
            mst_rc_syouhin.brand_code != manufacturers.url_rewrite
        )

        records = self.db.fetchall(q.get_sql())
        return records

    #10
    def getInsertUpdateModel(self):
        product_motor = Table('product_motor_shadow', 'eg_product' + self.tail)
        products = Table('products_shadow', 'eg_product' + self.tail)
        motors = Table('motors_shadow', 'eg_product' + self.tail)
        sys_code = Table('syouhin_sys_code', 'eg_product' + self.tail)
        mst_syouhin_model_info_all = Table('mst_syouhin_model_info_all', 'rc_products', 'model')
        mst_syouhin_model_info_all_count = Table('mst_syouhin_model_info_all', 'rc_products_count', 'model_count')
        mst_model = Table('mst_model', 'rc_products')

        qSub1 = Query.from_(products).join(
            product_motor, how=JoinType.inner
        ).on(
            product_motor.product_id == products.id
        ).join(
            motors, how=JoinType.inner
        ).on(
            motors.id == product_motor.motor_id
        ).select(
            products.sku,
            "GROUP_CONCAT(motors_shadow.url_rewrite ORDER BY CAST(motors_shadow.url_rewrite AS UNSIGNED) SEPARATOR '-') as model",
            fn.Count(motors.url_rewrite).as_('model_count')
        ).where(
            products.sku.isin(
                Query.from_(mst_syouhin_model_info_all_count).select(
                    mst_syouhin_model_info_all_count.syouhin_sys_code
                ).distinct().where(
                    mst_syouhin_model_info_all_count._type != 3
                )
            )
        ).groupby(
            products.sku
        )

        qSub2 = Query.from_(mst_syouhin_model_info_all).join(
            mst_syouhin_model_info_all_count, how=JoinType.inner
        ).on(
            (mst_syouhin_model_info_all.syouhin_model_code == mst_syouhin_model_info_all_count.syouhin_model_code) &
            (mst_syouhin_model_info_all.syouhin_sys_code == mst_syouhin_model_info_all_count.syouhin_sys_code)
        ).join(
            mst_model, how=JoinType.inner
        ).on(
            mst_syouhin_model_info_all.syouhin_model_code == mst_model.model_code
        ).select(
            mst_syouhin_model_info_all.syouhin_sys_code,
            "GROUP_CONCAT(model.syouhin_model_code ORDER BY CAST(model_count.syouhin_model_code AS UNSIGNED) SEPARATOR '-') as model_list",
            fn.Count(mst_syouhin_model_info_all.syouhin_model_code).as_('model_count')
        ).where(
            mst_syouhin_model_info_all_count._type != 3
        ).groupby(
            mst_syouhin_model_info_all.syouhin_sys_code
        )

        finds = ['"', 'products_shadow.GROUP_CONCAT','model.GROUP_CONCAT']
        replaces = ['', 'GROUP_CONCAT','GROUP_CONCAT']
        qSub1 = qSub1.get_sql()
        qSub2 = qSub2.get_sql()

        for find, replace in zip(finds, replaces):
            qSub1 = qSub1.replace(find, replace)
            qSub2 = qSub2.replace(find, replace)

        q = "SELECT real_tbl.sku, real_tbl.model as current_model, real_tbl.model_count, org_tbl.model_list as correct_model, org_tbl.model_count " + \
            "FROM ({}) as real_tbl".format(qSub1) + " " + \
            "INNER JOIN ({}) as org_tbl".format(qSub2) + " ON real_tbl.sku = org_tbl.syouhin_sys_code " + \
            "WHERE real_tbl.model <> org_tbl.model_list AND real_tbl.model_count <> org_tbl.model_count"

        records = self.db.fetchall(q)
        return records

    #11 THPM
    def getTotalImage(self):
        product_images = Table('product_images_shadow', 'eg_product' + self.tail)

        q = Query.from_(product_images).select(
            fn.Count('*')
        )

        record = self.db.fetchone(q.get_sql())
        return record

    #12 THPM
    def getStopProduct(self):
        products = Table('products', 'eg_product_staging')
        mst_stop_syouhin = Table('mst_stop_syouhin', 'gp_manager')

        q = Query.from_(products).join(
            mst_stop_syouhin, how=JoinType.inner
        ).on(
            products.sku == mst_stop_syouhin.syouhin_sys_code
        ).select(
            products.sku
        ).where(
            products.active == 1
        )

        records = self.db.fetchall(q.get_sql())
        return records

    #13 THPM
    def getStopBrand(self):
        products = Table('products', 'eg_product_staging')
        manufacturers = Table('manufacturers', 'eg_product_staging')
        mst_stop_brand = Table('mst_stop_brand', 'gp_manager')

        q = Query.from_(products).join(
            manufacturers, how=JoinType.left
        ).on(
            products.manufacturer_id == manufacturers.id
        ).join(
            mst_stop_brand, how=JoinType.inner
        ).on(
            mst_stop_brand.brand_code == manufacturers.url_rewrite
        ).select(
            products.sku, mst_stop_brand.brand_code
        ).where(products.active == 1)

        records = self.db.fetchall(q.get_sql())
        return records

    #14 THPM
    def getStopCate(self):
        products = Table('products', 'eg_product_staging')
        mst_stop_categories = Table('mst_stop_categories', 'gp_manager')

        q = Query.from_(products).join(
            mst_stop_categories, how=JoinType.inner
        ).on(
            products.category_id == mst_stop_categories.categories_id
        ).select(
            products.sku, mst_stop_categories.categories_id, products.active
        ).where(products.active == 1)

        records = self.db.fetchall(q.get_sql())
        return records

    #16 THPM - RealThai
    def getProductNotTitle(self):
        products = Table('products', 'eg_product_staging')
        product_translations = Table('product_translations', 'eg_product_staging')
        mst_rc_syouhin_multi = Table('mst_rc_syouhin_multi', 'rc_products_multi')

        q = Query.from_(products).join(
            product_translations, how=JoinType.left
        ).on(
            products.id == product_translations.product_id
        ).join(
            mst_rc_syouhin_multi, how=JoinType.left
        ).on(
            products.sku == mst_rc_syouhin_multi.syouhin_sys_code
        ).select(
            products.sku, product_translations.locale, product_translations.name, products.name, mst_rc_syouhin_multi.name.as_('sync_multi_name')
        ).where(
            fn.IsNull(product_translations.name) &
            products.active == 1
        )

        records = self.db.fetchall(q.get_sql())
        return records

    #17 
    def compareShippingPoint(self):
        tbl_syouhin_weight = Table('tbl_syouhin_weight', 'gp_manager')
        mst_syouhin_all = Table('mst_syouhin_all', 'eg_product_shadow')
        webike_products_to_categories = Table('webike_products_to_categories', 'rc_products')

        q = Query.from_(tbl_syouhin_weight).join(
            mst_syouhin_all, how=JoinType.inner
        ).on(
            tbl_syouhin_weight.syouhin_sku_code == mst_syouhin_all.syouhin_sku_code
        ).join(
            webike_products_to_categories,  how=JoinType.inner
        ).on(
            webike_products_to_categories.products_id == tbl_syouhin_weight.syouhin_sku_code
        ).select(
            tbl_syouhin_weight.syouhin_sku_code, 
            "round(gp_manager.tbl_syouhin_weight.weight / 100, 1)",
            mst_syouhin_all.weight_point.as_('goc')
        ).where(
            ((tbl_syouhin_weight.weight / 100) != mst_syouhin_all.weight_point) &
            (mst_syouhin_all.weight_point != 0) &
            (webike_products_to_categories.categories_id.isin([3001, 3002, 3003, 3005, 3006, 3017, 1002, 1003]))
        )

        finds = ['"', 'tbl_syouhin_weight.round']
        replaces = ['', 'round']

        q = q.get_sql()
        for find, replace in zip(finds, replaces):
            q = q.replace(find, replace)
        
        records = self.db.fetchall(q)
        return records

    #18
    def getProductsPrice_0(self):
        products = Table('products', 'eg_product_staging')
        q = Query.from_(products).select(
            products.sku,
            products.price,
            products._type 
        ).where(
            products.active == 1 &
            products.price.isin([0,1]),
            products._type != 3
        )

        records = self.db.fetchall(q.get_sql())
        return records

    #19
    def getManufactureNull(self):
        products = Table('products', 'eg_product_staging')
        q = Query.from_(products).select(
            '*'
        ).where(
            fn.IsNull(products.manufacturer_id) |
            products.manufacturer_id == ''
        )

        records = self.db.fetchall(q.get_sql())
        return records

    #20
    # def getProductOption(self):
    #     product_option = Table('product_option', 'eg_product_staging')