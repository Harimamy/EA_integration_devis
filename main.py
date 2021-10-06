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


def main_process(file_path):
    z, compteur_ligne, list_not_in_article, list_not_gamme = 0, 0, [], []
    try:
        if os.path.exists(file_path):
            df_input = get_df_from_file_input(path=file_path)
        else:
            Services.show_message_box(
                title='Integration SAGE',
                text="Le fichier {} à importer n'existe pas!".format(str(file_path)),
                style=0
            )
            raise SystemExit()
            sys.exit()
    except Exception as e:
        print("Error occured like, here :", e)
    Services.show_messagebox_auto_close(
        wait_in_milli_sec=4000,
        title_name="Integration SAGE",
        message="Vérification de la conformité du fichier en cours..."
    )
    dict_docentete, df_docligne = process_docentete_input(df_export=df_input), process_docligne_input(df_export=df_input)
    # dict_docentete, df_docligne = process_docentete_df(df_export=df_input), process_docligne_df(df_export=df_input)
    # connexion = connect_with_pyodbc_sql_server(server='RAVALOHERY-PC', database='ALU GESCOM SQL')
    # connexion = connect_with_pymssql(server='RAVALOHERY-PC', database='ALU GESCOM SQL')
    if os.path.exists(r'C:\Integration SAGE\server.ini'):
        with open(r'C:\Integration SAGE\server.ini', 'r') as file_connexion:
            list_info_connex = [item.strip() for item in file_connexion.readlines()]
    else:
        Services.show_message_box(
            title='Integration SAGE',
            text="Le fichier contenant les informations de connexion n'existe pas!",
            style=0
        )
        raise SystemExit()
        sys.exit()
    database_name = list_info_connex[1]
    connexion = connect_with_pymssql_login(
        server=list_info_connex[0],
        database=database_name,
        username=list_info_connex[2],
        password=list_info_connex[3]
    )
    try:
        connex = connexion.connect()
    except (Exception, ConnectionError, ConnectionRefusedError, ConnectionAbortedError) as connexion_error:
        Services.show_message_box(
            title="Integration SAGE",
            text="Erreur de connexion sur le serveur {}".format(list_info_connex[0]),
            style=0
        )
        raise SystemExit()
        sys.exit()
    # for this message box the best way is to use the msgbox in progress
    Services.show_messagebox_auto_close(wait_in_milli_sec=3000, title_name='Integration SAGE', message="Préparation d'import en cours...")
    dict_AR_design, dict_AR_unite = Services.prepare_df_articles(connexion=connexion)

    do_piece, optimisation, work_site = dict_docentete['piece_num'], dict_docentete['optimisation'], dict_docentete['worksite']
    date_document, client_name = dict_docentete['doc_date'], dict_docentete['client_name']
    time_now = datetime.now()
    logging.basicConfig(
        filename=r'''C:\Integration SAGE\Log\console_{}.log'''.format(do_piece),
        level=logging.DEBUG,
        format='%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s'
    )
    date_hour = "{}{:02d}{:02d}{:02d}".format("000", time_now.hour, time_now.minute, time_now.second)
    tuple_art_ref = tuple((str(ar_ref).strip() for ar_ref in df_docligne['Référence'] if pd.notna(ar_ref)))
    df_verification_stock = pd.read_sql_query(
        sql=f'''SELECT [AR_Ref],[AS_QteSto] FROM [{database_name}].[dbo].[F_ARTSTOCK] 
                WHERE [AS_QteSto] >= 0 and [DE_No] = 2 and [AR_Ref] IN {str(tuple_art_ref)}''',
        con=connexion)
    df_verification_gamstock = pd.read_sql_query(
        sql=f'''SELECT [AR_Ref],[AG_No1],[GS_QteSto] FROM [{database_name}].[dbo].[F_GAMSTOCK]
               WHERE [DE_No] = 2 and [AR_Ref] IN {str(tuple_art_ref)}''',
        con=connexion
    )
    dict_stock = {str(row['AR_Ref']): row['AS_QteSto'] for i, row in df_verification_stock.iterrows()}
    # getting each gamme for each qte in all article
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
        print("Voici la liste des articles qui ne sont pas disponible en stock, vérifier l'état de leurs stocks svp!\n", set(dict_docligne_qte) - set(dict_stock))
        Services.show_message_box(
            title="Integration SAGE",
            text="Articles non disponible en stock, vérifier l'état des stocks svp!\n {}".format(set(dict_docligne_qte) - set(dict_stock)),
            style=0
        )
        logging.warning('Warning error artilce(s) doesnt exist or lack of stock: {}'.format(set(dict_docligne_qte) - set(dict_stock)))
        raise SystemExit()
        sys.exit()
    # here is the comparison for each article from F_ARTSTOCK
    if all([compare(x=dict_stock[art_ref], y=dict_docligne_qte[art_ref]) for art_ref in tuple_art_ref]):
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
                    title_name="Integration SAGE",
                    message="Importation en cours..."
                )
                root_main = tk.Tk()
                root_main.geometry('370x120')
                root_main.title("Progression de l'importation!")
                # progressbar
                pb = ttk.Progressbar(
                    master=root_main,
                    orient='horizontal',
                    mode='determinate',
                    length=350
                )
                # place the progressbar
                pb.grid(column=0, row=0, columnspan=2, padx=10, pady=20)

                # label on the progress bar
                for i, row in df_docligne.iterrows():
                    value_label = ttk.Label(master=root_main, text=update_progress_label(
                        progressbar=pb,
                        art_ref=str(row["Référence"]),
                        check_last=True if i == df_docligne.index[-1] else False
                    ))
                    value_label.place(relx=0.5, rely=0.5, anchor='center')
                    value_label.grid(column=0, row=2, columnspan=2)
                    print(">♠ ", row["Référence"])
                    qte, mp_article = float(row["Qté"]), Services.associate_articles_proges_sage(row["Référence"])

                    art_gamme_no = Services.find_artgamme_no(
                        dict_corresp_ref=dict_art_ref,
                        color_gamme=row["Coloris"] if pd.isna(row["Coloris"]) else Services.auto_complete_gam(row["Coloris"]),
                        art_ref=str(row["Référence"])
                    )
                    DL_NoIn, DL_NoIn_eq, list_DL_NoIn_active = None, None, []
                    sql_artstock = f'''UPDATE [dbo].[F_ARTSTOCK] 
                                       SET [AS_QteSto] = [AS_QteSto] - {qte} 
                                       WHERE [AR_Ref] = '{row["Référence"]}' '''
                    sql_gamstock = f'''UPDATE [dbo].[F_GAMSTOCK] 
                                       SET [GS_QteSto] = [GS_QteSto] - {qte} 
                                       WHERE [AR_Ref] = '{row["Référence"]}' and [AG_No1] = {art_gamme_no}'''
                    if mp_article in dict_AR_design:
                        # if row['Référence'] in dict_art_ref:
                        # do_type=21 (only with 'mouvement de sortie', do_piece=no_piece, ar_ref='reference article', AG_No1=coloris, doc_entete
                        art_eu_enumere = Services.find_eu_enumere(num_unite=dict_AR_unite[mp_article])
                        sql_docentete = f'''INSERT INTO [dbo].[F_DOCENTETE]([DO_Domaine],[DO_Type],[DO_Piece],[DO_Date],[DO_Ref],[DO_Tiers],[CO_No],[DO_Period],
                                            [DO_Devise],[DO_Cours],[DE_No],[LI_No],[CT_NumPayeur],[DO_Expedit],[DO_NbFacture],[DO_BLFact],[DO_TxEscompte],
                                            [DO_Reliquat],[DO_Imprim],[CA_Num],[DO_Coord01],[DO_Coord02],[DO_Coord03],[DO_Coord04],[DO_Souche],[DO_DateLivr],
                                            [DO_Condition],[DO_Tarif],[DO_Colisage],[DO_TypeColis],[DO_Transaction],[DO_Langue],[DO_Ecart],[DO_Regime],
                                            [N_CatCompta],[DO_Ventile],[AB_No],[DO_DebutAbo],[DO_FinAbo],[DO_DebutPeriod],[DO_FinPeriod],[DO_Statut],[DO_Heure],
                                            [CA_No],[CO_NoCaissier],[DO_Transfere],[DO_Cloture],[DO_NoWeb],[DO_Attente],[DO_Provenance],[CA_NumIFRS],[MR_No],
                                            [DO_TypeFrais],[DO_ValFrais],[DO_TypeLigneFrais],[DO_TypeFranco],[DO_ValFranco],[DO_TypeLigneFranco],[DO_Taxe1],
                                            [DO_TypeTaux1],[DO_TypeTaxe1],[DO_Taxe2],[DO_TypeTaux2],[DO_TypeTaxe2],[DO_Taxe3],[DO_TypeTaux3],[DO_TypeTaxe3],
                                            [DO_MajCpta],[DO_Motif],[DO_Contact],[DO_FactureElec],[DO_TypeTransac],[Chantier],[Optimisation])
                                            VALUES( 2, 21, '{do_piece}', '{date_document}', '{client_name}', 2, 0, 0, 0, 0.0, 0, 0, '', 0, 0, 0, 0.0, 0, 0, '',
                                            '', '', '', '', 0, '1900-01-01 00:00:00', 0, 0, 1, 1, 0, 0, 0.0, 0, 0, 0, 0, '1900-01-01 00:00:00',
                                            '1900-01-01 00:00:00','1900-01-01 00:00:00', '1900-01-01 00:00:00', 0, '{date_hour}', 0, 0, 0, 0, '', 0, 0, '',
                                            0, 0, 0.0, 0, 0, 0.0, 0, 0.0, 0,0, 0.0, 0, 0, 0.0, 0, 0, 0, '', '', 0, 0, '{work_site}', '{optimisation}')'''

                        # sql_docentete = f'''INSERT INTO [dbo].[F_DOCENTETE] ([DO_Domaine],[DO_Type],[DO_Piece],[DO_Date],[DO_Ref],[DO_Tiers],[DE_No])
                        #                     VALUES (2, 21, '{do_piece}', '{date_document}', '{client_name}', 2, 2)'''

                        # here is to update the stock qte (artstock and gamstock)
                        try:
                            connex.execute(sql_artstock)
                            connex.execute(sql_gamstock)
                            print('two stock UPDATED!')
                        except Exception as e:
                            print("Exception catched, error occured as ", e)
                            logging.debug('Debug error about sql update artstock/gamstock catched: more detail... {}'.format(e))

                        dl_unit_price = Services.get_dl_pu(art_ref=mp_article, no_gam=art_gamme_no, connexion=connexion)
                        montant_ht = dl_unit_price * qte
                        sql_docligne = f'''INSERT INTO [dbo].[F_DOCLIGNE] ([DO_Domaine],[DO_Type],[CT_Num],[DO_Piece],[DL_PieceBC],[DL_PieceBL],[DO_Date],
                                           [DL_DateBC],[DL_DateBL],[DL_Ligne],[DO_Ref],[DL_TNomencl],[DL_TRemPied],[DL_TRemExep],[AR_Ref],[DL_Design],
                                           [DL_Qte],[DL_QteBC],[DL_QteBL],[DL_PoidsNet],[DL_PoidsBrut],[DL_Remise01REM_Valeur],[DL_Remise01REM_Type],
                                           [DL_Remise02REM_Valeur],[DL_Remise02REM_Type],[DL_Remise03REM_Valeur],[DL_Remise03REM_Type],[DL_PrixUnitaire],
                                           [DL_PUBC],[DL_Taxe1],[DL_TypeTaux1],[DL_TypeTaxe1],[DL_Taxe2],[DL_TypeTaux2],[DL_TypeTaxe2],[CO_No],[AG_No1],
                                           [AG_No2],[DL_PrixRU],[DL_CMUP],[DL_MvtStock],[DT_No],[AF_RefFourniss],[EU_Enumere],[EU_Qte],[DL_TTC],[DE_No],
                                           [DL_NoRef],[DL_PUDevise],[DL_PUTTC],[DO_DateLivr],[CA_Num],[DL_Taxe3],[DL_TypeTaux3],[DL_TypeTaxe3],[DL_Frais],
                                           [DL_Valorise],[DL_NonLivre],[AC_RefClient],[DL_MontantHT],[DL_MontantTTC],[DL_FactPoids],[DL_Escompte],[DL_PiecePL],
                                           [DL_DatePL],[DL_QtePL],[DL_NoColis],[DL_NoLink],[DL_QteRessource],[DL_TypePL],[DL_DateAvancement]) 
                                           VALUES (2, 21, 2, '{do_piece}', '', '', '{date_document}', '1900-01-01 00:00:00', '{date_document}', 10000, 
                                           '{client_name}', 0, 0, 0, '{mp_article}', '{Services.add_apostrophe(dict_AR_design[mp_article])}', 
                                           {qte}, {qte}, 0.0, 0.0, 0.0, 0.0, 0, 0.0, 0, 0.0, 0, {dl_unit_price}, 0.0, 0.0, 0, 0, 0.0, 0, 0, 0, {art_gamme_no}, 
                                           0, {dl_unit_price}, {dl_unit_price}, 3, 0, '', '{art_eu_enumere}', {qte}, 0, 2, 1, 0.0, 0.0, '1900-01-01 00:00:00', '', 
                                           0.0, 0, 0, 0.0, 1, 0,'', {montant_ht}, 0.0, 0, 0, '', '1900-01-01 00:00:00', {qte}, '', 0, 0, 0, '1900-01-01 00:00:00')
                                           SELECT SCOPE_IDENTITY();'''
                        query_get_lotfifo = f'''SELECT * FROM [{database_name}].[dbo].[F_LOTFIFO] 
                                    WHERE [AR_Ref] = '{row["Référence"]}' and [AG_No1] = {art_gamme_no} and [LF_MvtStock] = 1'''
                        # print(query_get_lotfifo)
                        df_mvt_in = pd.read_sql_query(
                            sql=query_get_lotfifo,
                            con=connexion
                        )

                        # you should get here the list of LF_QteRestant that applied substraction on Out Mvt
                        dict_qteRestant_DL_NoIn = {row['DL_NoIn']: row['LF_QteRestant'] for i, row in df_mvt_in.iterrows()}
                        list_DL_NoIn_active, dict_rest_no = sorted(list(df_mvt_in['DL_NoIn'])), {
                            row['DL_NoIn']: row['LF_QteRestant'] for i, row in df_mvt_in.iterrows()}
                        # print("here is the list DL_NoIn ", list_DL_NoIn_active)
                        # print("here is the dict_rest_no", dict_rest_no)

                        list_DL_NoIn_qte_restant = [(dl_no, dict_qteRestant_DL_NoIn[dl_no]) for dl_no in list_DL_NoIn_active]
                        try:
                            if dict_rest_no[list_DL_NoIn_active[0]] > qte:
                                # single processing one insert on lotfifo with only qte rest sup
                                DL_NoIn = list_DL_NoIn_active[0]
                                print("and the DL_NoIn is ", DL_NoIn)
                            elif dict_rest_no[list_DL_NoIn_active[0]] == qte:
                                # single processing one insert on lotfifo with only qte rest equal
                                DL_NoIn_eq = list_DL_NoIn_active[0]
                            else:
                                # multiple processing
                                count = 0
                                for dl_no in list_DL_NoIn_active:
                                    count += dict_rest_no[dl_no]
                                    list_DL_NoIn_active.append(dl_no)
                                    # insert + on lotfifo
                                    if count >= qte:
                                        print("the quantity is reached!! count ", count, "qte ", qte)
                                        break
                        except Exception as e:
                            print("an error occured as ", e)
                            logging.debug('Debug error catched: more detail... {}'.format(e))

                        # then select the min of cbMarq and get the DL_NoIn (DL_NoIn)
                        # get the last DL_No on docligne (DL_NoOut)

                        if z == 0:
                            try:
                                connex.execute(sql_docentete)
                                print("SUCCESS docentete")
                                z += 1
                            except Exception as e:
                                print("exception on sql_docentete catched as,", e)
                                logging.debug('Debug error catched about sql_docentete: more detail... {}'.format(e))
                                Services.show_messagebox_ui(
                                    title='Integration SAGE',
                                    message="Erreur de l'entête du document! vérifiez si le numero de pièce ou l'entête existe déjà"
                                )
                                raise SystemExit()
                                sys.exit()
                        else:
                            pass

                        try:
                            with connexion.begin() as new_connexion:
                                result_set = new_connexion.execute(sql_docligne)
                                value = str(result_set.mappings().all())
                            id_last_docligne = int([n for n in value.split("'") if n.isdigit()][0])
                            print("SUCCESS docligne")
                            compteur_ligne += 1
                            df_DL_NoOut = pd.read_sql_query(
                                sql=f'''SELECT [DL_No] FROM [{database_name}].[dbo].[F_DOCLIGNE] WHERE [cbMarq] = {id_last_docligne}''',
                                con=connexion)
                            DL_NoOut = df_DL_NoOut['DL_No'][0]

                        except Exception as e:
                            print("exception on sql_docligne catched as,", e)
                            logging.debug('Debug error about sql_docligne catched: more detail... {}'.format(e))

                        # sql_lotfifo_insert = f'''INSERT INTO [dbo].[F_LOTFIFO] ([AR_Ref],[AG_No1],[LF_Qte],[LF_QteRestant],[LF_LotEpuise],[DE_No],[DL_NoIn],
                        #                                                         [DL_NoOut],[LF_MvtStock],[LF_DateBL])
                        #                          VALUES ('{row["Référence"]}', {art_gamme_no}, {qte}, 0.0, 1, 2, {{}}, {DL_NoOut}, 3, '{date_document}')'''
                        #
                        # sql_lotfifo_insert_list = f'''INSERT INTO [dbo].[F_LOTFIFO] ([AR_Ref],[AG_No1],[LF_Qte],[LF_QteRestant],[LF_LotEpuise],[DE_No],[DL_NoIn],
                        #                                                              [DL_NoOut],[LF_MvtStock],[LF_DateBL])
                        #                               VALUES ('{row["Référence"]}', {art_gamme_no}, {{}}, 0, 1, 2, {{}}, {DL_NoOut}, 3, '{date_document}')'''

                        sql_lotfifo_update = f'''UPDATE [dbo].[F_LOTFIFO]
                                                 SET [LF_QteRestant] = [LF_QteRestant] - {qte}
                                                 WHERE [DL_NoIn] = {{}} and [AR_Ref] = '{row["Référence"]}' 
                                                 AND [AG_No1] = {art_gamme_no} AND [LF_LotEpuise] = 0 AND [DL_NoOut] = 0 AND [LF_MvtStock] = 1'''

                        sql_lotfifo_update_eq = f'''UPDATE [dbo].[F_LOTFIFO]
                                                    SET [LF_QteRestant] = [LF_QteRestant] - {qte},
                                                        [LF_LotEpuise] = 1
                                                    WHERE [DL_NoIn] = {{}} AND [AR_Ref] = '{row["Référence"]}' AND [AG_No1] = {art_gamme_no} 
                                                    AND [LF_LotEpuise] = 0 AND [DL_NoOut] = 0 AND [LF_MvtStock] = 1'''

                        sql_lotfifo_update_list0 = f'''UPDATE [dbo].[F_LOTFIFO]
                                                       SET [LF_QteRestant] = 0,
                                                           [LF_LotEpuise] = 1
                                                       WHERE [DL_NoIn] = {{}} AND [AR_Ref] = '{row["Référence"]}' AND [AG_No1] = {art_gamme_no} 
                                                       AND [LF_LotEpuise] = 0 AND [DL_NoOut] = 0 AND [LF_MvtStock] = 1'''

                        sql_lotfifo_update_list1 = f'''UPDATE [dbo].[F_LOTFIFO]
                                                         SET [LF_QteRestant] = [LF_QteRestant] - {{}}
                                                         WHERE [DL_NoIn] = {{}} AND [AR_Ref] = '{row["Référence"]}' AND [AG_No1] = {art_gamme_no} 
                                                         AND [LF_LotEpuise] = 0 AND [DL_NoOut] = 0 AND [LF_MvtStock] = 1;'''

                        # here is to update/insert into the lotfifo
                        try:
                            # print("in the update/insert lotfifo", all((bool(DL_NoIn), not bool(DL_NoIn_eq), not bool(list_DL_NoIn_active))))
                            print(DL_NoIn, "--", DL_NoIn_eq, "--", list_DL_NoIn_active)
                            if all((bool(DL_NoIn), not bool(DL_NoIn_eq), bool(list_DL_NoIn_active))):
                                # UPDATE
                                # print(DL_NoIn, "Verified, congratulations!!")
                                connex.execute(sql_lotfifo_update.format(int(DL_NoIn)))

                                # INSERT
                                # connex.execute(sql_lotfifo_insert.format(int(DL_NoIn)))

                            elif all((not bool(DL_NoIn), bool(DL_NoIn_eq), bool(list_DL_NoIn_active))):
                                # UPDATE
                                connex.execute(sql_lotfifo_update_eq.format(int(DL_NoIn_eq)))

                                # INSERT
                                # connex.execute(sql_lotfifo_insert.format(int(DL_NoIn_eq)))

                            elif all((not bool(DL_NoIn), not bool(DL_NoIn_eq), bool(list_DL_NoIn_active))):
                                sum_lot_qte = 0
                                for dl_no_in in list_DL_NoIn_qte_restant:
                                    if dl_no_in[0] != list_DL_NoIn_qte_restant[-1][0]:
                                        # UPDATE
                                        connex.execute(sql_lotfifo_update_list0.format(int(dl_no_in[0])))
                                        sum_lot_qte += dl_no_in[1]

                                        # INSERT
                                        # connex.execute(sql_lotfifo_insert_list.format(dl_no_in[1], int(dl_no_in[0])))

                                    else:
                                        # UPDATE
                                        qte_substract_final = qte - sum_lot_qte
                                        connex.execute(sql_lotfifo_update_list1.format(qte_substract_final, int(dl_no_in[0])))

                                        # INSERT
                                        # connex.execute(sql_lotfifo_insert_list.format(qte_substract_final, int(dl_no_in[0])))

                            else:
                                print("You have a miracle process!!..")
                        except Exception as e:
                            print("You have an exception here as ", e)
                            # raise Exception('exception catched here when e is ', e)
                            logging.error('Error about insert/update f_lotfifo catched: more detail... {}'.format(e))
                    else:
                        list_not_in_article.append(row['Référence'])

                    # dealing vitrage
                    if str(row['Référence']).startswith('VT'):
                        #         print(row["Qté"]*(-1))
                        art_ref_vt = Services.recover_vitre_art_ref(
                            vt_name=row["Référence"],
                            vertec_list=Services.get_list_ref_vertec()
                        )
                        # here is to update the stock qte for vitrage (artstock and gamstock)
                        try:
                            connex.execute(sql_artstock)
                            connex.execute(sql_gamstock)
                            print('two stock UPDATED!')
                        except Exception as e:
                            print("Exception catched, error occured as ", e)
                            logging.debug('Debug error about two stock update artstock/gamstock VT catched: more detail... {}'.format(e))

                        unit_price_vt = Services.get_dl_pu(art_ref=mp_article, no_gam=art_gamme_no, connexion=connexion)
                        vt_montant_ht = unit_price_vt * qte
                        sql_docligne = f'''INSERT INTO [dbo].[F_DOCLIGNE] ([DO_Domaine],[DO_Type],[CT_Num],[DO_Piece],[DL_PieceBC],[DL_PieceBL],[DO_Date],
                                           [DL_DateBC],[DL_DateBL],[DL_Ligne],[DO_Ref],[DL_TNomencl],[DL_TRemPied],[DL_TRemExep],[AR_Ref],[DL_Design],
                                           [DL_Qte],[DL_QteBC],[DL_QteBL],[DL_PoidsNet],[DL_PoidsBrut],[DL_Remise01REM_Valeur],[DL_Remise01REM_Type],
                                           [DL_Remise02REM_Valeur],[DL_Remise02REM_Type],[DL_Remise03REM_Valeur],[DL_Remise03REM_Type],[DL_PrixUnitaire],
                                           [DL_PUBC],[DL_Taxe1],[DL_TypeTaux1],[DL_TypeTaxe1],[DL_Taxe2],[DL_TypeTaux2],[DL_TypeTaxe2],[CO_No],[AG_No1],
                                           [AG_No2],[DL_PrixRU],[DL_CMUP],[DL_MvtStock],[DT_No],[AF_RefFourniss],[EU_Enumere],[EU_Qte],[DL_TTC],[DE_No],
                                           [DL_NoRef],[DL_PUDevise],[DL_PUTTC],[DO_DateLivr],[CA_Num],[DL_Taxe3],[DL_TypeTaux3],[DL_TypeTaxe3],[DL_Frais],
                                           [DL_Valorise],[DL_NonLivre],[AC_RefClient],[DL_MontantHT],[DL_MontantTTC],[DL_FactPoids],[DL_Escompte],[DL_PiecePL],
                                           [DL_DatePL],[DL_QtePL],[DL_NoColis],[DL_NoLink],[DL_QteRessource],[DL_TypePL],[DL_DateAvancement]) 
                                           VALUES (2, 21, 2, '{do_piece}', '', '', '{date_document}', '1900-01-01 00:00:00', '{date_document}', 10000, 
                                           '{client_name}', 0, 0, 0, '{art_ref_vt}', '{Services.add_apostrophe(dict_AR_design[art_ref_vt])}', 
                                           {qte}, {qte}, 0.0, 0.0, 0.0, 0.0, 0, 0.0, 0, 0.0, 0, {unit_price_vt}, 0.0, 0.0, 0, 0, 0.0, 0, 0, 0, {art_gamme_no}, 
                                           0, {unit_price_vt}, {unit_price_vt}, 3, 0, '', 'm²', {qte}, 0, 2, 1, 0.0, 0.0, '1900-01-01 00:00:00', '', 0.0, 0, 0, 
                                           0.0, 1, 0,'', {vt_montant_ht}, 0.0, 0, 0, '', '1900-01-01 00:00:00', {qte}, '', 0, 0, 0, '1900-01-01 00:00:00')
                                            SELECT SCOPE_IDENTITY();'''
                        try:
                            for value in connex.execute(sql_docligne):
                                id_last_docligne_VT = value[0]
                            print("SUCCESS docligne")
                            compteur_ligne += 1
                        except Exception as e:
                            print("exception on sql_docligne catched as,", e)
                            logging.debug('Debug error about docligne VT catched: more detail... {}'.format(e))

                    try:
                        pb.update()
                        # if i == len(df)
                        # the error came here, you should manage progress on the bar for successing the process in import SAGE
                        progress(
                            # var=math.ceil(100/len(df_docligne)) if i == df_docligne.index[-1] else (100/len(df_docligne)),
                            var=100/len(df_docligne),
                            root_pb=root_main,
                            progressbar=pb,
                            value_label=value_label,
                            article=mp_article,
                            check=True if i == df_docligne.index[-1] else False
                        )
                        time.sleep(0.1)
                        # root_main.destroy()
                    except (TclError, RuntimeError, Exception) as te:
                        print("You have an TclError occured! ", te)
                        logging.error('Error catched as TclError exception: more detail... {}'.format(te))
                        pass
                root_main.destroy()
                root_main.mainloop()
                print(compteur_ligne, 'éxecuté(s) avec SUCCES!!')
                Services.show_message_box(
                    title="Integration SAGE",
                    text=f"{compteur_ligne} lignes importées avec succès!",
                    style=0
                )
                print(list_not_in_article, 'la liste darticle qui ne sont pas dans f_article!!')
                print(list_not_gamme, 'la liste qui na pas de gamme!')
                connex.close()
                connexion.dispose()
                # here is to generate the log file and drop or remove the file imported after all operation successfully!
                os.remove(path=file_path)
            else:
                list_lack_gam = [
                    art_ref for art_ref in dict_gamstock if not compare(
                        x=dict_gamstock[art_ref][int(Services.find_artgamme_no(
                            dict_corresp_ref=dict_art_ref,
                            color_gamme=dict_art_ref_gam[art_ref],
                            art_ref=str(art_ref)
                        ))],
                        y=dict_docligne_qte[art_ref]
                    )
                ]
                Services.show_message_box(
                    title='Integration SAGE',
                    text=f"Vérifiez s'il vous plait, stock insuffisant pour chacun de ces articles sur la gamme demandée! {list_lack_gam}",
                    style=0
                )
        except (KeyError, TypeError, Exception) as ex:
            print("An error occurred here on all compared on each gam in each article as: ", ex)
            logging.ERROR("Error disappointed here maybe the article is in gamme and normally he shouldn't be! ")
    else:
        list_lack_of_article = [art_ref for art_ref in tuple_art_ref if not compare(x=dict_stock[art_ref], y=dict_docligne_qte[art_ref])]
        print("Lack of the stock on: ", list_lack_of_article)
        Services.show_message_box(
            title="Integration SAGE",
            text=f"Vérifiez s'il vous plait, vous avez des stocks manquants sur ces articles: {list_lack_of_article}",
            style=0
        )


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

    dict_AR_design, dict_AR_unite = Services.prepare_df_articles(connexion=connexion)
    df_docligne = process_docligne_df(df_export=devis_from_export)

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
        print("Voici la liste des articles qui ne sont pas disponible en stock, vérifier l'état de leurs stocks svp!\n",
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
