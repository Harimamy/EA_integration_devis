import logging
import math
import os
from datetime import datetime

import pandas as pd

from services.cipher_from_AES import AESCipher
from services.services import Services
from services.connexion_to_sql_server import connect_with_pymssql, connect_with_pymssql_login


def update_progress_label(progressbar, art_ref, check_last=None):
    if check_last:
        return "                          \nImportation terminée 100%"
    return f"Article: {art_ref} \nProgression: {math.ceil(progressbar['value'])}%"


def progress(var, progressbar, value_label, article, check):
    if progressbar['value'] < 100:
        progressbar['value'] += var
        value_label['text'] = ''
        value_label['text'] = update_progress_label(progressbar=progressbar, art_ref=article, check_last=check)
    else:
        # showinfo(message='The progress completed!')
        progressbar.stop()


def get_df_from_file_input(path):
    sheet_name = pd.ExcelFile(path).sheet_names[0]
    df_excel_from_export = pd.read_excel(
        io=path,
        sheet_name=sheet_name,
        header=None
    )
    return df_excel_from_export


# it's valid only on the process for inserting the devis from PROGES
def process_docentete_df(df_export):
    dict_docentete, client_name, do_piece, document_date, do_ref = dict(), None, None, None, None
    for i, (index, row) in enumerate(df_export.iterrows()):
        for col in df_export.columns:
            if any([not document_date, pd.isna(document_date)]):
                try:
                    document_date = pd.to_datetime(row[col].split()[-1])
                    print("date document -- ", document_date) if pd.notnull(document_date) else None
                    next
                except:
                    print('You have an error to convert datetime from the export excel!')

            elif str(row[col]).__contains__("Référence"):
                do_ref = row[col].split(':')[1].strip()
                print("here is do_ref", do_ref)

            elif type(row[col]) == str:
                try:
                    if all([row[col].__contains__("nom client"), row[col].__contains__(":")]):
                        # there is a little problem here if the first word is not the name of the client
                        client_name = Services.add_apostrophe(row[col].split(':')[1].strip()) if client_name is None else client_name

                    elif row[col].__contains__("Devis N"):
                        do_piece = row[col].split()[-1] if do_piece is None else do_piece



                except Exception as e:
                    print("Exception catched ", e)
                    pass
        if all((client_name, do_piece, document_date)):
            print("Client -- ", client_name)
            print("No piece or No Devis -- ", do_piece)
            print("Référence --", do_ref)
            dict_docentete['do_date'], dict_docentete['client_name'] = document_date, client_name
            dict_docentete['do_piece'], dict_docentete['do_ref'] = do_piece, do_ref
            break
    return dict_docentete


def process_docligne_df(df_export):
    dict_df_docligne = dict()
    print(df_export.columns)
    for i, (index, row) in enumerate(df_export.iterrows()):
        index_col_first, index_col_last = None, None
        for col in df_export.columns:
            try:
                if col != df_export.columns[-1]:
                    if all((str(row[col]).__contains__('Coloris'), str(row[col]).strip().isalpha(), row[col + 1] == 'Qté')):
                        for iter_col in range(len(df_export.columns)):
                            if row[iter_col] == 'Coloris':
                                index_col_first = iter_col
                            if str(row[iter_col]) == 'P.T. TTC':
                                index_col_last = iter_col + 1
                                break
                print(index_col_first, index_col_last) if index_col_first is not None and index_col_last is not None else None
                if index_col_first is not None and index_col_last is not None:
                    print(i, index_col_first, index_col_last)
                    df_docligne = df_export.iloc[i:, index_col_first:index_col_last]
                    new_header = df_docligne.iloc[0]
                    print("on iter_col to define header of the dataframe for DL")
                    df_docligne = df_docligne[1:]
                    df_docligne.columns = new_header
                    last_row = Services.cut_df_end(df_docligne)
                    df_docligne = df_docligne.iloc[:last_row, :][[col for col in df_docligne.columns if pd.notna(col)]]
                    df_docligne = df_docligne.dropna()
                    print(df_docligne)
    #                     dict_df_docligne[i] = df_docligne
            except Exception as e:
                raise("You have an exception catched, that's on ", e)
    return df_docligne


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    time_now = datetime.now()
    path = r'''D:\sage donnees\ALU\Devis 1\base_DEVIS.xlsx'''
    devis_from_export = pd.read_excel(path, sheet_name=pd.ExcelFile(path).sheet_names[0], header=None)
    dict_docentete = process_docentete_df(df_export=devis_from_export)
    try:
        class_aes = AESCipher('7ql9zA1bqqSnoYnt4zw3HppY')
    except Exception as e:
        Services.show_message_box("Erreur d'instance sur l'encryptage AES sur le mot de passe et utilisateur de la base Gestion Commerciale!")
        print("Erreur d'instance sur l'encryptage AES...", e)
        logging.ERROR("Erreur d'instance sur l'encryptage AES...")
        raise SystemExit()
        sys.exit()
    if os.path.exists(r'C:\Integration SAGE\server.ini'):
        with open(r'C:\Integration SAGE\server.ini', 'r') as file_connexion:
            list_info_connex = [item.strip() for item in file_connexion.readlines()]
    else:
        Services.show_message_box(
            title='Integration SAGE',
            text="Le fichier contenant les informations de connexion n'existe pas! Merci de contacter l'administrateur",
            style=0
        )
        raise SystemExit()
        sys.exit()
    database_name = list_info_connex[1]
    # connexion = connect_with_pymssql_login(
    #     server=list_info_connex[0],
    #     database=database_name,
    #     username=class_aes.decrypt(list_info_connex[2]),
    #     password=class_aes.decrypt(list_info_connex[3])
    # )

    connexion = connect_with_pymssql(server='RAVALOHERY-PC', database='ALU_SQL')
    do_piece, date_document, do_ref, deposit = dict_docentete['do_piece'], dict_docentete['do_date'], dict_docentete['do_ref'], 1
    logging.basicConfig(
        filename=r'''C:\Integration SAGE\DEVIS\Log\console_{}.log'''.format(do_piece),
        level=logging.DEBUG,
        format='%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s'
    )

    # client_code = dict_docentete['client_name']
    client_code = 'MOURTAZA'
    client_name = Services.find_code_client(client_code, connexion=connexion)
    date_hour = date_hour = "{}{:02d}{:02d}{:02d}".format("000", time_now.hour, time_now.minute, time_now.second)
    work_site = 'test_chantier'
    try:
        connex = connexion.connect()
    except Exception as e:
        print("An error occurred on the connexion into the server! as ", e)
    # the new method for writing the sql query is to define the number of field to equal with the number of the value field
    sql_docentete = f"""INSERT INTO   [dbo].  [F_DOCENTETE]([DO_Domaine],  [DO_Type],  [DO_Piece],  [DO_Date],  [DO_Ref],  [DO_Tiers],  
                        [CO_No],  [DO_Period],  [DO_Devise],  [DO_Cours],  [DE_No],  [LI_No],  [CT_NumPayeur],  [DO_Expedit],  
                        [DO_NbFacture],  [DO_BLFact],  [DO_TxEscompte],  [DO_Reliquat],  [DO_Imprim],  [CA_Num],  [DO_Coord01],  
                        [DO_Coord02],  [DO_Coord03],  [DO_Coord04],  [DO_Souche],  [DO_DateLivr],  [DO_Condition],  [DO_Tarif],  
                        [DO_Colisage],  [DO_TypeColis],  [DO_Transaction],  [DO_Langue],  [DO_Ecart],  [DO_Regime],  [N_CatCompta],  
                        [DO_Ventile],  [AB_No],  [DO_DebutAbo],  [DO_FinAbo],  [DO_DebutPeriod],  [DO_FinPeriod],  [DO_Statut],  
                        [DO_Heure],  [CA_No],  [CO_NoCaissier],  [DO_Transfere],  [DO_Cloture],  [DO_NoWeb],  [DO_Attente],  
                        [DO_Provenance],  [CA_NumIFRS],  [MR_No],  [DO_TypeFrais],  [DO_ValFrais],  [DO_TypeLigneFrais],  
                        [DO_TypeFranco],  [DO_ValFranco],  [DO_TypeLigneFranco],  [DO_Taxe1],  [DO_TypeTaux1],  [DO_TypeTaxe1],  
                        [DO_Taxe2],  [DO_TypeTaux2],  [DO_TypeTaxe2],  [DO_Taxe3],  [DO_TypeTaux3],  [DO_TypeTaxe3],  [DO_MajCpta],  
                        [DO_Motif],  [DO_Contact],  [DO_FactureElec],  [DO_TypeTransac],  [Chantier],  [Optimisation])
                        VALUES(0, 0, '{do_piece}', '{date_document}', '{do_ref[-17:]}', '{client_name}', 
                               0, 1, 0, 0.0, {deposit}, 0, '{client_name}', 1, 
                               1, 0, 0.0, 0, 0, '', '', 
                               '', '', '', 0, '1900-01-01 00:00:00', 1, 1, 
                               1, 1, 11, 0, 0.0, 21, 1, 
                               0, 0, '1900-01-01 00:00:00', '1900-01-01 00:00:00', '1900-01-01 00:00:00', '1900-01-01 00:00:00', 1,
                               '{date_hour}', 0, 0, 0, 0, '', 0, 
                               0, '', 0, 0, 0.0, 0, 
                               0, 0.0, 0, 0.0, 0, 0,
                               0.0, 0, 0, 0.0, 0, 0, 0, 
                               '', '', 0, 0, '{work_site}', '')"""
    try:
        connex.execute(sql_docentete)
        print("SUCCESS docentete!!")
    except Exception as e:
        print('Error occurred on the execution of the sql docentete, the detail is ', e)

    dict_AR_design, dict_AR_unite, df_article = Services.prepare_df_articles(connexion=connexion)
    df_docligne = process_docligne_df(df_export=devis_from_export)
    df_art_follow_stock = df_article.loc[df_article['AR_SuiviStock'] == 0][['AR_Ref', 'AR_SuiviStock']]

    # Here is to add the art_ref for each PF
    df_docligne['Référence'] = [dict_AR_design[design] for design in df_docligne['Désignation']]
    print(df_docligne.columns)
    try:
        set_articles_concerned = {dict_AR_design[str(art_design).strip()] for art_design in df_docligne['Désignation']}
        dict_art_ref = Services.get_dict_art_ref_gamme(connexion=connexion, include_article=set_articles_concerned)
    except Exception as ex:
        print("An exception catched here as ", ex)

    # STOCK control
    df_verification_stock = pd.read_sql_query(
        sql=f'''SELECT [AR_Ref],[AS_QteSto] FROM [{database_name}].[dbo].[F_ARTSTOCK] 
                    WHERE [AS_QteSto] >= 0 and [DE_No] = {deposit} and [AR_Ref] IN {str(tuple(set_articles_concerned))}''',
        con=connexion
    )
    df_verification_gamstock = pd.read_sql_query(
        sql=f'''SELECT [AR_Ref],[AG_No1],[GS_QteSto] FROM [{database_name}].[dbo].[F_GAMSTOCK]
                   WHERE [DE_No] = {deposit} and [AR_Ref] IN {str(tuple(set_articles_concerned))}''',
        con=connexion
    )

    dict_stock = {str(row['AR_Ref']): row['AS_QteSto'] for i, row in df_verification_stock.iterrows()}
    # getting each gamme for each qte in all article (PF)
    dict_gamstock = {
        str(row['AR_Ref']): {row1['AG_No1']: row1['GS_QteSto']
                             for index, row1 in df_verification_gamstock.loc[df_verification_gamstock['AR_Ref'] == row['AR_Ref']].iterrows()}
        for i, row in df_verification_gamstock.iterrows()
    }

    dict_docligne_qte = {str(row["Référence"]): float(row["Qté"]) for i, row in df_docligne.iterrows() if pd.notna(row["Référence"])}
    compare = lambda x, y: x > y
    if set(dict_docligne_qte) == set(dict_stock):
        print("Vérification des articles en stock disponible... Ok")
        Services.show_messagebox_auto_close(
            wait_in_milli_sec=4000,
            title_name="Integration SAGE",
            message="Vérification des articles en stock disponible..."
        )
    else:
        print("Assurez-vous que le(s) produit(s) fini(s) existe(nt) dans la base SAGE gestion commerciale, puis vérifier l'état de leurs stocks s'il vous plait!\n",
              set(dict_docligne_qte) - set(dict_stock))
        Services.show_message_box(
            title="Integration SAGE",
            text="Articles non disponible en stock, vérifier l'état des stocks svp!\n {}".format(set(dict_docligne_qte) - set(dict_stock)),
            style=0
        )
        logging.warning('Warning error artilce(s) doesnt exist or lack of stock: {}'.format(set(dict_docligne_qte) - set(dict_stock)))
        raise SystemExit()
        sys.exit()

    # here is the comparison for each article from F_ARTSTOCK
    if all([compare(x=dict_stock[art_ref], y=dict_docligne_qte[art_ref]) for art_ref in set_articles_concerned]):
        # and after that here is the comparison for each article on gamme from F_GAMSTOCK
        df_docligne['Référence'] = [str(article).strip() for article in df_docligne['Référence']]
        set_articles_concerned = {Services.associate_articles_proges_sage(str(article).strip()) for article in df_docligne['Référence']}
        dict_art_ref = Services.get_dict_art_ref_gamme(connexion=connexion, include_article=set_articles_concerned)
        dict_art_ref_gam = {ar_ref['Référence']: Services.auto_complete_gam(ar_ref['Coloris'])
                            for index, ar_ref in df_docligne.iterrows() if pd.notna(ar_ref['Coloris'])}
        try:
            if all([
                compare(
                    x=dict_gamstock[art_ref][int(Services.find_artgamme_no(
                        dict_corresp_ref=dict_art_ref,
                        color_gamme=dict_art_ref_gam[art_ref],
                        art_ref=str(art_ref)
                    ))],
                    y=dict_docligne_qte[art_ref]
                ) for art_ref in dict_gamstock
            ]):
                Services.show_messagebox_auto_close(
                    wait_in_milli_sec=3000,
                    title_name="Integration DEVIS",
                    message="Importation en cours..."
                )

        # the EXCEPT of exception should be here

                for i, row in df_docligne.iterrows():
                    unit_cost_price = 0.0
                    dl_cmup = 'NULL'
                    qte = row['Qté']
                    art_ref_pf = dict_AR_design[row['Désignation']]
                    art_eu_enumere = Services.find_eu_enumere(num_unite=dict_AR_unite[art_ref_pf])
                    dl_unit_price, dl_pu_ttc = row['P.U. TTC'], row['P.U. TTC']
                    montant_ht = dl_unit_price * qte
                    width, height = row['L'], row['H']
                    art_gamme_no = Services.find_artgamme_no(
                                    dict_corresp_ref=dict_art_ref,
                                    color_gamme=row["Coloris"] if pd.isna(row["Coloris"]) else Services.auto_complete_gam(row["Coloris"]),
                                    art_ref=art_ref_pf
                                )
                    # should verify the DL_Valorise because, this part 1 if compose and 0 if not
                    # the another is for DL_PUTTC
                    sql_docligne = f"""INSERT INTO [dbo].[F_DOCLIGNE] ([DO_Domaine],[DO_Type],[CT_Num],[DO_Piece],[DL_PieceBC],[DL_PieceBL],[DO_Date],
                                       [DL_DateBC],[DL_DateBL],[DL_Ligne],[DO_Ref],[DL_TNomencl],[DL_TRemPied],[DL_TRemExep],[AR_Ref],[DL_Design],
                                       [DL_Qte],[DL_QteBC],[DL_QteBL],[DL_PoidsNet],[DL_PoidsBrut],[DL_Remise01REM_Valeur],[DL_Remise01REM_Type],
                                       [DL_Remise02REM_Valeur],[DL_Remise02REM_Type],[DL_Remise03REM_Valeur],[DL_Remise03REM_Type],[DL_PrixUnitaire],
                                       [DL_PUBC],[DL_Taxe1],[DL_TypeTaux1],[DL_TypeTaxe1],[DL_Taxe2],[DL_TypeTaux2],[DL_TypeTaxe2],[CO_No],[AG_No1],
                                       [AG_No2],[DL_PrixRU],[DL_CMUP],[DL_MvtStock],[DT_No],[AF_RefFourniss],[EU_Enumere],[EU_Qte],[DL_TTC],[DE_No],
                                       [DL_NoRef],[DL_PUDevise],[DL_PUTTC],[DO_DateLivr],[CA_Num],[DL_Taxe3],[DL_TypeTaux3],[DL_TypeTaxe3],[DL_Frais],
                                       [DL_Valorise],[AR_RefCompose],[DL_NonLivre],[AC_RefClient],[DL_MontantHT],[DL_MontantTTC],[DL_FactPoids],[DL_Escompte],[DL_PiecePL],
                                       [DL_DatePL],[DL_QtePL],[DL_NoColis],[DL_NoLink],[DL_QteRessource],[DL_TypePL],[DL_DateAvancement],[Largeur],[Hauteur]) 
                                       VALUES (0, 0, '{client_name}', '{do_piece}', '', '', '{date_document}', 
                                       '1900-01-01 00:00:00', '{date_document}', 10000, '{do_ref[-17:]}', 0, 0, 0, '{art_ref_pf}', '{row['Désignation']}', 
                                       {qte}, {qte}, 0.0, 0.0, 0.0, 0.0, 1, 
                                       0.0, 0, 0.0, 0, {dl_unit_price}, 
                                       0.0, 20.0, 0, 0, 0.0, 0, 0, 1, {art_gamme_no},
                                       0, {unit_cost_price}, {dl_cmup}, 0, 0, '', '{art_eu_enumere}', {qte}, 0, {deposit},
                                       1, 0.0, {dl_pu_ttc}, '1900-01-01 00:00:00', '', 0.0, 0, 0, 0.0, 
                                       1, NULL, 0,'', {montant_ht}, 0.0, 0, 0, '', 
                                       '1900-01-01 00:00:00', 0.0, '', 0, 0, 0, '1900-01-01 00:00:00', {width}, {height})"""

                    # executing the docligne in progress
                    try:
                        # don't like this line
                        connex.execute(sql_docligne)
                        print("SUCCESS DOCLIGNE")
                    except Exception as e:
                        print("an error occurred on sql docligne executed!", e)
                        raise
        except Exception as ex:
            print("excpetion catched here as ", e)

    print("DEVIS importé SUCCES!!...")
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
