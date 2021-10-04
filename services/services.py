import pandas as pd


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

    @staticmethod
    def prepare_df_articles(connexion):
        df_article = Services.read_sql_speed_up(query='''SELECT * FROM [dbo].[F_ARTICLE]''', db_engine=connexion)
        dict_design_article = {row['AR_Design']: str(row['AR_Ref']) for i, row in df_article.iterrows()}
        dict_article_unite = {str(row['AR_Ref']): row['AR_UniteVen'] for i, row in df_article.iterrows()}
        return dict_design_article, dict_article_unite

    # the only error that occured is if the article is not in gamme on the file export from proges
    # and the same article is in gamme on sage
    @staticmethod
    def find_artgamme_no(dict_corresp_ref, color_gamme, art_ref):
        if any((art_ref not in dict_corresp_ref, pd.isna(color_gamme))):
            return '0'
        for i in dict_corresp_ref[art_ref]:
            if color_gamme in i:
                return i[0]


    @staticmethod
    def find_code_client(client_name, connexion):
        df_client = pd.read_sql_query(r'SELECT * FROM [dbo].[F_COMPTET] WHERE [CT_Type] = 0', con=connexion)
        dict_client = {row['CT_Intitule']: row['CT_Num'] for i, row in df_client.iterrows()}
        # for cli in df_client['CT_Intitule']:
        #     # if client_name in cli:
        #     # regex
        #     pass
        return dict_client[client_name]


    @staticmethod
    def get_dict_art_ref_gamme(connexion, include_article):
        full_artgamme = Services.read_sql_speed_up(query='''SELECT * FROM [dbo].[F_ARTGAMME]''', db_engine=connexion)
        required_artgamme = full_artgamme.loc[full_artgamme['AR_Ref'].isin(include_article)][['AR_Ref', 'AG_No', 'EG_Enumere']]
        set_art_ref = set(required_artgamme['AR_Ref'])
        print("here is to generate dict comprehension, it takes about some minutes")
        # you can use this option when this part is running at once before executing the programme complete with full_art_gamme
        # the last required_artgamme = full_artgamme[['AR_Ref', 'AG_No', 'EG_Enumere']] when not using the include_article
        dict_art_ref = {
            key: tuple((row['AG_No'], row['EG_Enumere']) for i, row in required_artgamme.iterrows() if row['AR_Ref'] == key)
            for key in set_art_ref
        }
        return dict_art_ref

        # here is about the association of color gamme correction
    @staticmethod
    def auto_complete_gam(color):
        dict_associate_color = {
            'Bichromate': 'Bichromate',
            'Brute': 'Brut',
            'Brut': 'Brut',
            'Noir': 'Noir',
            'Noire': 'Noir',
            'Pur white': 'Pure white',
            'Pure white': 'Pure white',
            'Blanc': 'Blanc',
            'Blanc RAL9010': 'Blanc RAL9010',
            'Bronze': 'Bronze',
            'Bronz': 'Bronze',
            'Gris anthracite': 'Gris anthracite',
            'Imitation bois': 'Imitation bois',
            'Inox': 'Inox',
            'Zingue': 'Zingue'
        }
        return dict_associate_color[str(color).strip().capitalize()]

    # this is used only for the "matiere premiere" in french version of variable
    @staticmethod
    def associate_articles_proges_sage(art_ref_from_proges):
        dict_associate_articles = {
            '7030 -EA': '7030',
            '7301 -EA': '7301',
            '7302 -EA': '7302',
            'A 801': 'A801',
            'A 802': 'A802',
            'A 820': 'A820',
            'A 831': 'A831',
            'A 890': 'A890',
            'A 891': 'A891',
            'A 895': 'A895',
            'A CON': 'ACON',
            'A COVER': 'ACOV',
            'A LEG': 'ALEG',
            'A2,120': 'A2.120',
            'A2,135': 'A2.135',
            'A2,90': 'A2.90',
            'AA30*20-14': 'AA_20X30-14',
            'AR45*100-18': 'AR_45X100-18',
            'CL003-70': 'CL003',
            'CL003B': 'CL003-60',
            'CLM35X35': 'CL035_EVA',
            'CR1 15*1': 'CR1-15',
            'CR1 15X1': 'CR1-15',
            'CR1 7*1': 'CR1-7_MV',
            # 'CR1 7*1': 'CR1_7X1',
            # 'CR1 7X1': 'CR1_7X1',
            'CR200MEVA': 'CR200-FDC',
            'CR319': 'CR319-NU',
            'FHG41 30CM': 'FHG41',
            'KIT CRM1': 'KITCRM1',
            'KIT J1': 'KIT-J1',
            'PC70X700X3P': 'PC70X700X3PEVA',
            'SCLEMEVA': 'SERMV02',
            'SS102-3,5': 'SS102-3.5',
            'SS102-4,0': 'SS102-4.0',
            'SS201-2,5': 'SS201-2.5',
            'SS 203': 'SS203',
            'SS202-1,5': 'SS202-1.5',
            'SS905-4,5': 'SS905',
            'VIS 4,2*45': '42X45',
            'VIS 4,2*13 TB': '4213TB',
            'VIS 4,2*13 TF': '4213TFC',
            'VIS 4,2*22 TF': '4222',
            'VIS 4,2*45': '4245',
            'VIS 4,2X13 TB': '4213TB',
            'VIS 4,2X13 TBA': 'VIS4.2X13TBA',
            'VIS 4,8*25 TB': '4825',
            'VIS 4,8*50': '4850',
            'VIS 4,8X25 TB': '4825'
        }
        return dict_associate_articles[art_ref_from_proges] if art_ref_from_proges in dict_associate_articles else art_ref_from_proges

    @staticmethod
    def find_eu_enumere(num_unite):
        # dict_unite = {
        #     1: 'Pièce',
        #     2: 'Mètre',
        #     3: 'm²',
        #     4: 'Paire',
        #     5: 'mm',
        #     6: 'Barre 5.80',
        #     7: 'Heure',
        #     8: 'Forfait'
        # }
        dict_unite = {
            1: 'Unité',
            2: 'Pièce',
            3: 'Paire',
            4: 'Mètre',
            5: 'Barre',
            6: 'Barre 5.80',
            7: 'Heure',
            8: 'Forfait'
        }
        if num_unite not in dict_unite:
            return Services.show_message_box(
                title="message EA_integration_SAGE",
                text="Erreur! le numero d'unité de vente ne correspond pas à la liste d'unité existante",
                style=0
            )
        return dict_unite[num_unite]

    # you should use this function for testing if the article is compose or not

