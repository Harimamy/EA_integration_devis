class Services:
    @staticmethod
    def add_apostrophe(item):
        if "'" in item:
            return item.replace("'", "''")
        else:
            return item

    @staticmethod
    def cut_df_end(df):
        for i, (index, row) in enumerate(df.iterrows()):
            if str(row['Coloris']).__contains__('Devis'):
                return i