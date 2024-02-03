import sys
import time
import pandas as pd
import psycopg2 as pg
import numpy as np
import unicodedata

# para importacao de dataframes no postgresql
from sqlalchemy import create_engine, text, update, MetaData

# para gerar nanoid para o public_id das tabelas (exposição para o browser)
from nanoid import generate

# password Hashing
import bcrypt

# correios
import brazilcep # novo nome da pycep_correios
from brazilcep import get_address_from_cep, WebService, exceptions

# to send email from Python
import smtplib, ssl


# # credenciais BD
from dotenv import load_dotenv
load_dotenv("./.env.dev")

# import .env.dev as cred
import os
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")


########################################################################################################################
#                     AUXILIARES

def generate_nanoid(size=1, len=8) :
    return [generate(size=len) for i in range(0,size)]

def generate_cryptpass(senha,salt) :
    # Hash a password for the first time, with a randomly-generated salt
    hashed = bcrypt.hashpw(senha.encode(), salt)
    return hashed

def trim_special_chars(str,chars=['.','-',' ','(',')'],newchar='') :
    for char in chars :
        str = str.replace(char,newchar)
    return str
    
def add_separator(str,oldsep,newsep) :
    return str.replace(oldsep, newsep)

def remove_duplicates(kw,flag=1) :
    if flag :
        kw = trim_special_chars(kw)
    lst = kw.split(';')
    lst = map(str.strip, lst) 
    lst = list(dict.fromkeys(lst)) 
    return lst

def trunc_string(str,stopword) :
    str_temp = trim_special_chars(str.lower(),chars=['-',',','.'],newchar=' ')
    tam = len(str_temp.split(stopword)[0])
    return str.strip()[:tam-2].strip()

def split_stringlist(lst,stopword):
    l_str1 = []
    l_str2 = []
    for l in lst:
        str1, str2 = split_string(l,stopword)
        l_str1.append(str1)
        l_str2.append(str2)
    return l_str1,l_str2

def split_string(str,stopword) :
    pos = str.lower().find(stopword.lower())
    if pos < 0:
        before = trim_special_chars(str,chars=['-',',','.'],newchar=' ').strip()
        return [before,None]
    before,after = str[0:pos],str[pos+len(stopword)+1:]
    before = trim_special_chars(before,chars=['-',',','.'],newchar=' ').strip()
    after = trim_special_chars(after,chars=['-',',','.'],newchar=' ').strip()
    return [before, after]

def getEngine() :
    # insert into db
    # sqlalchemy engine
    return create_engine('postgresql://' + USER + ':' + PASSWORD + '@' + HOST + ':' + PORT + '/' + DATABASE)

def get_addressFromCEP(df_ceps) :
    df_ceps = df_ceps.drop_duplicates().dropna()

    lst_ruas = []
    lst_cidades = []
    lst_ufs = []
        
    for row in df_ceps.itertuples() :        
        cep = row[1]
        
        ws = WebService.VIACEP
        try:
            # DESCOMENTAR PROX LINHA
            endereco = get_address_from_cep(cep, webservice=ws)
            # json de teste 
            # endereco = {'district': 'rua abc', 'cep': '37503130', 'city': 'city ABC', 'street': 'str', 'uf': 'str', 'complement': 'str'} #
            lst_cidades.append(endereco['city'])
            lst_ufs.append(endereco['uf'])
            if endereco['street'] == '' :
                lst_ruas.append("pega da planilha")
            else :
                lst_ruas.append(endereco['street'])

        # when provide CEP is invalid. Wrong size or with chars that dont be numbers.
        except exceptions.InvalidCEP as eic:
            print(eic)
            # print('Na linha: ', idx+2)
            print('CEP inválido',cep)
            lst_cidades.append('')
            lst_ufs.append('')
            lst_ruas.append("pega da planilha")
        #     lst_cidades.append('CEP inválido') A PEDIDO: IGNORAR
        #     lst_ufs.append('CEP inválido')
        #     lst_ruas.append('CEP inválido')
            continue
        # CEP is fine but not exist ou not found in request API's database
        except exceptions.CEPNotFound as ecnf:
            print(ecnf)
            print('CEP não encontrado',cep)
            lst_cidades.append('')
            lst_ufs.append('')
            lst_ruas.append('pega da planilha')
            # lst_cidades.append('CEP não encontrado') A PEDIDO: IGNORAR
            # lst_ufs.append('CEP não encontrado')
            # lst_ruas.append('CEP não encontrado')
            continue
        # many request exception
        except exceptions.BlockedByFlood as ebbf:
            print(ebbf)
            continue
        # general exceptions
        except exceptions.BrazilCEPException as e:
            print(e)
            continue

    if len(df_ceps) == len(lst_cidades) == len(lst_ruas) == len(lst_ufs) :
        df_ceps.insert(1,"uf",lst_ufs)
        df_ceps.insert(1,"county",lst_cidades)
        df_ceps.insert(1,"rua",lst_ruas)

    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options 
    #     print(df_ceps)
    
    return df_ceps

# https://stackoverflow.com/questions/517923/what-is-the-best-way-to-remove-accents-normalize-in-a-python-unicode-string
# https://stackoverflow.com/questions/53222476/how-to-remove-the-%C3%A2-xa0-from-list-of-strings-in-python
def strip_accents(s):
   return ''.join(c for c in unicodedata.normalize('NFKD', str(s))
                  if unicodedata.category(c) != 'Mn')

def trim_allcolumns(df) :
    for (colname,colval) in df.iteritems():
        df[colname] = colval.astype(str).apply(lambda x: x.strip()) #eplace(' ', ''))
    return df

def sendEmail(df) :
    # credenciais SMTP + GMAIL
    port = 587  # For starttls
    smtp_server = "smtp.gmail.com"
    sender_email = "sahudy.montenegro@gmail.com"  # noreply-pea-pescarte@uenf.br
    password = "xeux czgq uekt dwvb"  
    SUBJECT= "Login na plataforma Pescarte"  
    TEXT = """\
    Cara(o) %s %s,
    
    Segue seu login/senha para acessar a plataforma Pescarte (pescarte.uenf.br). 
    login: %s (seu CPF)
    senha: %s
    
    Att., 
    
    Equipe Plataforma Pescarte
    """
    # .format(SUBJECT, TEXT)
    message = f'Subject: {SUBJECT}\n\n{TEXT}'

    context = ssl.create_default_context()
    for idx,u in df.iterrows() :
        if u[5] :    #  'ATIVO (S/N)'
            cpf = u[0].strip()
            nome = strip_accents(u[1].strip().replace(u'\xa0', u''))
            sobrenome = strip_accents(u[2].strip().replace(u'\xa0', u''))
            senha = u[3] # "Senha"
            # receiver_email = u[4]  # "E-mail" do usuario       DESCOMENTAR 
            # apenas teste! comentar ou apagar a prox linha!!!
            receiver_email = "sahudy@ufscar.br"  # "annabell@uenf.br" "zoey.spessanha@icloud.com"
            with smtplib.SMTP(smtp_server, port) as server:
                # server.ehlo()  # Can be omitted
                server.starttls(context=context)
                # server.ehlo()  # Can be omitted
                server.login(sender_email, password)
                server.sendmail(sender_email, receiver_email, message % (nome,sobrenome,cpf,senha))
    print("All users got e-mail!")
    # server.quit()
    return 

########################################################################################################################
#                    CONEXÃO
def connectDB() :
    
    try: 
        conn = pg.connect(
            host=HOST,
            port=PORT,
            database=DATABASE,
            user=USER,
            password=PASSWORD)
        print("Connection established")
        return conn

    except (Exception, pg.DatabaseError) as error:
        print("Erro na conexão: " + error)
        if conn is not None:
            conn.close()
            print('Database connection closed.')
        sys.exit()

#               DADOS DA GOOGLE SHEET
def connect2Data () :

    # ler Google Sheet
    #https://docs.google.com/spreadsheets/d/1MftO9ypxQ70hF32bC24DMjiuGq9frWC42wOojEczg4Q/edit?usp=sharing
        
    SHEET_ID = '1MftO9ypxQ70hF32bC24DMjiuGq9frWC42wOojEczg4Q'
    SHEET_NAME = 'lista'
    url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}'
    df_sheet = pd.read_csv(url)
    # print(df_sheet.columns)
    # print(df_sheet.head())

    return df_sheet
    
    
    ########################################################################################################################
#               NÚCLEOS DE PESQUISA
def insertNucleos(conn) :

    # Nucleos de Pesquisa 
    # script estático
    #     NUCLEO A - CULTURA
    #     NUCLEO B - RECURSOS HÍDRICOS E ALIMENTARES
    #     NUCLEO C - SOCIABILIDADES E PARTICIPAÇÃO
    #     NUCLEO D - CENSO E AFINS

    cur = conn.cursor()
    sqltxt = 'INSERT INTO nucleo_pesquisa (nome, "desc", id_publico, letra, inserted_at, updated_at) VALUES (%s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s)) ON CONFLICT DO NOTHING'

    vals = ('CULTURA', 'NUCLEO A - CULTURA', generate_nanoid()[0], 'A', time.time(), time.time())
    cur.execute(sqltxt,vals)

    vals = ('RECURSOS HÍDRICOS E ALIMENTARES', 'NUCLEO B - RECURSOS HÍDRICOS E ALIMENTARES',  generate_nanoid()[0], 'B', time.time(), time.time())
    cur.execute(sqltxt,vals)

    vals = ('SOCIABILIDADES E PARTICIPAÇÃO', 'NUCLEO C - SOCIABILIDADES E PARTICIPAÇÃO',  generate_nanoid()[0], 'C', time.time(), time.time())
    cur.execute(sqltxt,vals)

    vals = ('CENSO E AFINS', 'NUCLEO D - CENSO E AFINS',  generate_nanoid()[0], 'D', time.time(), time.time())
    cur.execute(sqltxt,vals)

    conn.commit()
    cur.close()
    print('Insert NUCLEOS sucessfull.')

########################################################################################################################
#               LINHAS DE PESQUISA
    
def insertLPs(conn,df_sheet) :

    df_linhaspesquisa = df_sheet[['NÚCLEO','NoLP','NOME LINHA DE PESQUISA']] #,'Responsavel pela LP? (insira o numero da LP)']]
    df_linhaspesquisa = df_linhaspesquisa.drop_duplicates()

    # trim string
    df_obj = df_linhaspesquisa.select_dtypes(['object'])
    df_linhaspesquisa[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
    df_linhaspesquisa = df_linhaspesquisa.rename(columns={"NÚCLEO": "nucleo", "NoLP": "number", "NOME LINHA DE PESQUISA": "short_desc"}) #, "Responsavel pela LP? (insira o numero da LP)": "responsavel_lp_id"})

    # public_id - usando nanoid size=10 https://pypi.org/project/nanoid/
    nanoids = generate_nanoid(len(df_linhaspesquisa.index))
    df_linhaspesquisa.insert(0,"public_id",nanoids, allow_duplicates=False)
    df_linhaspesquisa.fillna('null', inplace=True) # nucleo_pesquisa_letra com NaN

    # importando para o BD - tabela linha_pesquisa
    sqltxt = "INSERT INTO linha_pesquisa (id_publico, numero, desc_curta, nucleo_pesquisa_letra, inserted_at, updated_at) VALUES (%s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s))" 

    cur = conn.cursor()
    for idx,lp in df_linhaspesquisa.iterrows() :
        vals = (lp['public_id'], lp['number'], lp['short_desc'], lp['nucleo'], time.time(), time.time())
        cur.execute(sqltxt,vals)
    # cur.execute("UPDATE linha_pesquisa SET nucleo_pesquisa_letra = null WHERE nucleo_pesquisa_letra = 'None'")  # updating null values
    conn.commit()
    cur.close()
    print("LPs inserted")


########################################################################################################################
#                     AUXILIARES SQLs

def get_userID(conn, cpf) :
    cur = conn.cursor()
    sqltxt = 'SELECT id_publico FROM usuario ' + "WHERE cpf ILIKE '%s'" % cpf
    cur.execute(sqltxt)
    user_id = cur.fetchone()
    if user_id == None :
        cur.close()
        return 0
    cur.close()
    return user_id[0]

def get_campusID(conn, univ_sigla, campus) :
    cur = conn.cursor()
    sqltxt = "SELECT id_publico FROM campus WHERE acronimo ILIKE '%s' AND nome ILIKE '%s'" % (univ_sigla.strip(), campus.strip())
    cur.execute(sqltxt)
    campus_id = cur.fetchone()
    cur.close()
    if campus_id == None :
        return None
    return campus_id[0]

def get_pesquisadorID(conn,lattes) :
    cur = conn.cursor()
    sqltxt = "SELECT id_publico FROM pesquisador WHERE link_lattes ILIKE '%s'" % lattes
    cur.execute(sqltxt)
    pesquisador_id = cur.fetchone()
    cur.close()
    if pesquisador_id == None:
        return None
    return pesquisador_id[0]

def get_table_rowcount(conn,table) :
    cur = conn.cursor()
    sql_query = "SELECT count(*) FROM " + table
    cur.execute(sql_query)
    records = cur.fetchall()
    cur.close()
    return records[0][0]

def empty_table(conn,table,ini=0) :
    row_count = get_table_rowcount(conn,table)
    cur = conn.cursor()
    if row_count != 0 :
        if ini == 0 :
            sql_query = "DELETE FROM " + table
        else :
            sql_query = "DELETE FROM " + table + " WHERE id >= %s" % ini
        cur.execute(sql_query)
    conn.commit()
    print("Old rows removed...")
    cur.close()

def erase_data(conn) :
    empty_table(conn,'"pesquisador_LP"')
    empty_table(conn,'pesquisador')
    empty_table(conn,'usuario')
    empty_table(conn,"contato")
    empty_table(conn,"campus")
    empty_table(conn,"endereco")
    empty_table(conn,'linha_pesquisa')
    empty_table(conn,'nucleo_pesquisa')
    
    # vacuum(conn)
    print("LPs + NUCLEOS + CAMPI + ADDRESS + CONTACT + User + Pesquisador e suas LPs => ALL DELETED")

def vacuum(self): # nao funciona, quem eh self???
    old_isolation_level = self.conn.isolation_level
    self.conn.set_isolation_level(0)
    query = "VACUUM FULL"
    self._doQuery(query)
    self.conn.set_isolation_level(old_isolation_level)


########################################################################################################################
#                     UNIVERSIDADES + CAMPUS

def insertCampi(conn,df_sheet) :
     # endereço dos campi
    df_universidades_complement = df_sheet[['UNIVERSIDADE','UNIVERSIDADE SIGLA','CAMPUS','ENDEREÇO - CAMPUS (sem cidade e nem cep)','CEP - CAMPUS']].drop_duplicates().dropna(subset=['CEP - CAMPUS'])
    df_universidades_address = insertUniversityAddress(conn,df_universidades_complement)
    df_universidades_core = df_universidades_complement[['UNIVERSIDADE','UNIVERSIDADE SIGLA','CAMPUS']] #,"id"]]
    df_universidades_core = df_universidades_core.rename(columns={"UNIVERSIDADE": "university_name", "UNIVERSIDADE SIGLA": "initials", "CAMPUS": "name"})#, "id": "address_id"})

    # applies the replacement to all column values in a pandas dataframe at once.
    df_universidades_core = trim_allcolumns(df_universidades_core)
    df_universidades_core.drop_duplicates(inplace=True)

    # acrescentando o id public como nanoid
    nanoids = generate_nanoid(len(df_universidades_core.index))
    df_universidades_core.insert(0,"public_id",nanoids, allow_duplicates=False)

    sqltxt = "INSERT INTO campus (id_publico, acronimo, nome, nome_universidade, endereco_id, inserted_at, updated_at) VALUES (%s, %s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s))" 

    cur = conn.cursor()
    for idx,campus in df_universidades_core.iterrows() :
        address_id = df_universidades_address.at[idx,"public_id"]
        vals = (campus['public_id'], campus['initials'], campus['name'], campus['university_name'], address_id, time.time(), time.time())
        cur.execute(sqltxt,vals)

    conn.commit()
    cur.close()
    print("Campi inserted")

def insertUniversityAddress(conn,df_universidades_complement) :
    # limpando CEP: só números
    df_cep = pd.DataFrame(df_universidades_complement['CEP - CAMPUS'].apply(lambda row: trim_special_chars(row)).astype(str).drop_duplicates())
    print("Universities: Checking CEP against BrazilCEP - Brazilian zip codes...")
    df_address = get_addressFromCEP(df_cep)

    # quando o CEP é invalido, nao encontrado 
    for idx,ad in df_address.iterrows() :
        # a rua na planilha eh mais completa, no CEP-BR nao vem numero da rua (of course)
        df_address.at[idx,'rua'] = df_universidades_complement.at[idx,'ENDEREÇO - CAMPUS (sem cidade e nem cep)' ]
        if ad[2] == '' :
            df_address.at[idx,'county'] = df_universidades_complement.at[idx,'CAMPUS']
            df_address.at[idx,'uf'] = 'RJ' # nao tem UF da universidade na planilha, os 2 casos em q  cep nao da certo sao de RJ

    df_address.insert(0,"public_id",generate_nanoid(len(df_address.index)), allow_duplicates=False)

    sqltxt_address = "INSERT INTO endereco (id_publico, rua, cep, cidade, estado, inserted_at, updated_at) VALUES (%s, %s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s))"

    cur = conn.cursor()
    for idx, ad in df_address.iterrows() :
        vals_address = (ad['public_id'], ad['rua'], ad['CEP - CAMPUS'], ad['county'], ad['uf'], time.time(), time.time())
        cur.execute(sqltxt_address, vals_address)

    conn.commit()
    cur.close()
    print("Addresses inserted")
    return df_address


########################################################################################################################
#                     USUARIOS     +     PESQUISADORES

def insertContactInfo(conn,df_contato) :
    cur = conn.cursor()

    sqltxt_address = "INSERT INTO endereco (id_publico, rua, cep, cidade, estado, inserted_at, updated_at) VALUES (%s, %s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s))"
    sqltxt_contato = "INSERT INTO contato (id_publico, celular_principal, email_principal, celulares_adicionais, emails_adicionais, endereco_id, inserted_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s))"

    for idx,contact in df_contato.iterrows() :
        # inserir o endereco
        vals_address = (contact['public_id'], contact['Endereço'], contact['CEP'], contact['county'], contact['uf'], time.time(), time.time())
        cur.execute(sqltxt_address, vals_address)

        # inserir contato com FK do endereco
        if contact['Outros telefones'] == '{None}' :
            contact['Outros telefones'] = None
        if contact['Outros E-mails'] == '{None}' :
            contact['Outros E-mails'] = None
        vals_contato = (contact['public_id'], contact['Telefone'], contact['E-mail'], contact['Outros telefones'], contact['Outros E-mails'], contact['public_id'], time.time(), time.time())
        cur.execute(sqltxt_contato, vals_contato)

    conn.commit()
    cur.close()
    print("Telefones + E-mail + Address inserted")
    return df_contato

def insertUserData(conn,df_usuarios) :
    cur = conn.cursor()

    # garantindo formato de DATA DMY
    sql = "SET datestyle to DMY, SQL; "
    cur.execute(sql)
    sqltxt = 'INSERT INTO usuario (id_publico, cpf, rg, primeiro_nome, sobrenome, data_nascimento, tipo, hash_senha, "ativo?", link_avatar, contato_id, inserted_at, updated_at) \
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s))'

    for idx,u in df_usuarios.iterrows() :
        vals = (u['public_id'], u['CPF'], u['RG'], \
            u['BOLSISTA - NOME'], u['BOLSISTA - SOBRENOME'],    \
            u['Data de Nascimento'], "pesquisador", u['Hashed'], u['ATIVO (S/N)'], u['Foto'], u['contato_id'], time.time(), time.time())
        cur.execute(sqltxt, vals)

    conn.commit()
    cur.close()
    print("User's core data inserted")

def insertDadosPesquisa(conn,df_pesquisa) :
    cur = conn.cursor()

    # garantindo formato de DATA DMY
    sql = "SET datestyle to DMY, SQL; "
    cur.execute(sql)
    sqltxt = 'INSERT INTO pesquisador (id_publico, bolsa, link_lattes, formacao, \
          data_inicio_bolsa, data_fim_bolsa, data_contratacao, campus_id, usuario_id, inserted_at, updated_at) \
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s))'

    for idx,u in df_pesquisa.iterrows() :
        campus_id = get_campusID(conn,u['UNIVERSIDADE SIGLA'], u['CAMPUS']) 
        pesquisador_id = get_userID(conn,u['CPF'])
        vals = (pesquisador_id, u['TIPO DE BOLSA'], \
                u['LINK LATTES'], u['FORMAÇÃO'], \
                    u['Data de início BOLSA'], u['Data de fim BOLSA'], u['DATA DA CONTRATAÇÃO'], \
                    campus_id, pesquisador_id, time.time(), time.time())
        cur.execute(sqltxt, vals)

    conn.commit()
    cur.close()
    print("Dados específicos da pesquisa inserted")

def insert_LP_pesquisador(conn,df_LP):
    cur = conn.cursor()
    sqltxt = 'INSERT INTO "pesquisador_LP" (pesquisador, linha_pesquisa, "lider?", inserted_at, updated_at) \
                VALUES (%s, %s, %s, to_timestamp(%s), to_timestamp(%s))'
    for idx,lp in df_LP.iterrows() :
        pesquisador_id = get_pesquisadorID(conn,lp['LINK LATTES'])
        lider = False
        if lp['Responsavel pela LP? (insira o numero da LP)'] == lp['NoLP'] :
            lider = True
        vals = (pesquisador_id, lp['NoLP'],lider, time.time(), time.time())
        cur.execute(sqltxt, vals)

    conn.commit()
    cur.close()
    print("LPs dos pesquisadores inseridas com LIDER")

def arrange_contact_info(conn,df_usuarios_contato) :
    # precisa extrair a partir do bairro (nao mais) e cidade do endereço
    address = df_usuarios_contato['Endereço']
    address_OK, cidade = split_stringlist(address,'cidade')
    df_usuarios_contato['Endereço'] = address_OK
    # nao vamos mais separar o bairro do endereco completo
    df_usuarios_contato.insert(3,'Cidade',cidade)

    # juntar telef e email de varias linhas do mesmo pesquisador
    df_usuarios_contato.update(pd.DataFrame(df_usuarios_contato['Telefone'].apply(lambda row: add_separator(row,' (',';('))))
    df_usuarios_contato.update(pd.DataFrame(df_usuarios_contato['E-mail'].apply(lambda row: add_separator(row,',',';'))))

    df_usuarios_contato.drop_duplicates(inplace=True,keep='first')

    # separando em dois campos: first|rest
    df_usuarios_contato.update(pd.DataFrame(df_usuarios_contato['Telefone'].astype(str).apply(lambda row: trim_special_chars(row,chars=['.','-',' ','(',')'])))) #  outros possiveis a remover
    df_usuarios_contato[['Telefone','Outros telefones']] = pd.DataFrame(df_usuarios_contato['Telefone']\
                .str.split(';', n=1, expand=True, regex=False)\
                .applymap(lambda value: None if value == "" else value),     
                index= df_usuarios_contato.index) # `.applymap` converts empty strings ("") into None.
    df_usuarios_contato.update(pd.DataFrame(df_usuarios_contato['Outros telefones'].astype(str).apply(lambda row: "{" + row + "}")))
    # (email,outros_emails) 
    df_usuarios_contato.update(pd.DataFrame(df_usuarios_contato['E-mail'].astype(str).apply(lambda row: trim_special_chars(row,chars=[' ']))))
    df_usuarios_contato[['E-mail','Outros E-mails']] = pd.DataFrame(df_usuarios_contato['E-mail']\
                .str.split(';', n=1, expand=True, regex=False)\
                .applymap(lambda value: None if value == "" else value),     
                index= df_usuarios_contato.index)
    df_usuarios_contato.update(pd.DataFrame(df_usuarios_contato['Outros E-mails'].astype(str).apply(lambda row: "{" + row + "}")))

    # limpando CEP: só números e dai removendo duplicatas que tinham CEP com caracteres especiais diferentes
    df_usuarios_contato['CEP'] = df_usuarios_contato['CEP'].apply(lambda row: trim_special_chars(row)).astype(str)
    df_usuarios_contato.drop_duplicates(inplace=True,keep='first')

    print('Users: Checking CEP against BrazilCEP - Brazilian zip codes...')
    df_address = get_addressFromCEP(df_usuarios_contato[['CEP']]).astype(str)

    # quando o CEP é invalido, nao encontrado 
    for idx,ad in df_address.iterrows() :
        # a rua na planilha eh mais completa, no CEP-BR nao vem numero da rua (of course)
        df_address.at[idx,'rua'] = df_usuarios_contato.at[idx,'Endereço' ]
        if ad[2] == '' : # CEP errado vem sem cidade e UF, precisa completar com os dados da planilha 
            df_address.at[idx,'county'] = df_usuarios_contato.at[idx,'Cidade']
            # uf = get_UF(conn,df_address.at[idx,'county']) #df_usuarios_contato.at[idx,'UF'] # nao tem UF da universidade na planilha, os 2 casos em q  cep nao da certo sao de RJ
            # if uf != 0 :
            #     df_address.at[idx,'uf'] =  uf  
            # else :
            df_address.at[idx,'uf'] = 'RJ' # nao tem UF da universidade na planilha, os 2 casos em q  cep nao da certo sao de RJ

    df_usuarios_contato = df_usuarios_contato.join(df_address.set_index('CEP'), on='CEP', how='left')

    # acrescentando o nanoid unico da tabela
    df_usuarios_contato.insert(0,"public_id",generate_nanoid(len(df_usuarios_contato)), allow_duplicates=False)

    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options 
    #     print(df_usuarios_contato)
    #     print(len(df_usuarios_contato))

    return df_usuarios_contato

def verify_dataconsistency(df_sheet) :
    # BEGIN CONSISTENCY
    df_nomes = df_sheet[['BOLSISTA']].drop_duplicates().dropna()

    df_cpf = df_sheet[['CPF']].dropna()
    df_cpf.update(pd.DataFrame(df_cpf['CPF'].apply(lambda row: trim_special_chars(row)).astype(str)))
    df_cpf.drop_duplicates(inplace=True)
   
    df_rg = df_sheet[['RG']].dropna()
    df_rg.update(pd.DataFrame(df_rg['RG'].apply(lambda row: trim_special_chars(row)).astype(str)))
    df_rg.drop_duplicates(inplace=True)

    if df_nomes.size != df_cpf.size != df_rg.size :
        return -1
    # END CONSISTENCY
   
    return 1

def loadUsuarios(conn,df_sheet) :

    if not verify_dataconsistency(df_sheet) :
        print('Dados inválidos - Revise o dataset')
        return

    # dados da tabela CONTATO
    df_usuarios_contato = arrange_contact_info(conn,df_sheet[['Endereço', 'CEP', 'Telefone', 'E-mail']])
    # # insere todas as info de contato: endereco completo, telefones, emails
    insertContactInfo(conn,df_usuarios_contato[['public_id', 'Endereço', 'CEP', 'Telefone',\
        'E-mail', 'Outros telefones', 'Outros E-mails', 'county', 'uf']]) # em casos mais genericos precisa passar o 'uf' para conferir unicidade de cidade

    # inserindo o restante dos dados na tabela de usuarios com perfil pesquisador
    df_usuarios = df_sheet[['CPF', 'RG', 'BOLSISTA - NOME', \
        'BOLSISTA - SOBRENOME', \
        'ATIVO (S/N)', 
        'Data de Nascimento', 'Foto', 'Telefone']]
    df_usuarios.update(pd.DataFrame(df_usuarios['CPF']\
        .apply(lambda row: trim_special_chars(row)).astype(str))) 
    df_usuarios.update(pd.DataFrame(df_usuarios['RG'].astype(str)\
        .apply(lambda row: trim_special_chars(row)).astype(str))) 

    df_usuarios['ATIVO (S/N)'].mask(df_usuarios['ATIVO (S/N)']=='S', other=True, inplace=True)
    df_usuarios['ATIVO (S/N)'].mask(df_usuarios['ATIVO (S/N)']=='N', other=False, inplace=True)

    # removendo duplicatas que tinham CPF com caracteres especiais diferentes
    df_usuarios.drop_duplicates(inplace = True)
    # inserir id do contato para usar no usuario e o email para mandar email com a senha de usuario
    df_usuarios = df_usuarios.join(df_usuarios_contato[['public_id', 'E-mail']],how='inner')
    df_usuarios.rename(columns={'public_id': 'contato_id'}, inplace=True) 

    # gerando public_id do usuario
    nanoids = generate_nanoid(len(df_usuarios.index))
    df_usuarios.insert(0,"public_id",nanoids, allow_duplicates=False)

    # criando passwords para users
    senhas = generate_nanoid(len(df_usuarios.index))
    df_usuarios.insert(7,'Senha',senhas, allow_duplicates=False)
    print('Encriptying passwords...')
    salt = bcrypt.gensalt()
    df_usuarios['Hashed'] = df_usuarios['Senha'].apply(lambda row: generate_cryptpass(row,salt))

    # TESTANDO A SENHA E SUA BCRYPT        
    # for row in df_usuarios.itertuples() :
    #     if bcrypt.checkpw(row[8].encode(), row[9]) :
    #         print("It Matches!")
    #     else:
    #         print("It Does not Match :(")

    insertUserData(conn,df_usuarios) 
    #  DESCOMENTAR ABAIXO
    sendEmail(df_usuarios[['CPF', 'BOLSISTA - PRIMEIRO NOME', \
                           'BOLSISTA - ÚLTIMO NOME', 'Senha', 'E-mail', 'ATIVO (S/N)']]) # envia email com a senha de usuario para o email principal

    # inserindo os dados na tabela de pesquisadores
    df_pesquisadores = df_datasheet[['CPF', 'TIPO DE BOLSA', 'FORMAÇÃO', 'DATA DA CONTRATAÇÃO', \
                                     'LINK LATTES', 'UNIVERSIDADE SIGLA', 'CAMPUS',  \
                                     'Data de início BOLSA', 'Data de fim BOLSA']]
    df_pesquisadores.update(pd.DataFrame(df_pesquisadores['CPF']\
        .apply(lambda row: trim_special_chars(row)).astype(str))) 
    df_pesquisadores.drop_duplicates(inplace=True,keep='first')
    # gerando public_id do pesquisador
    nanoids = generate_nanoid(len(df_pesquisadores.index))
    df_pesquisadores.insert(0,"public_id",nanoids, allow_duplicates=False)

    df_pesquisadores['Data de fim BOLSA'].where(~df_pesquisadores['Data de fim BOLSA'].isna(), None, inplace=True)

    insertDadosPesquisa(conn, df_pesquisadores)

    # inserindo as LPs por pesquisador
    df_pesquisadores_LPs = df_datasheet[['NoLP', 'LINK LATTES', 'Responsavel pela LP? (insira o numero da LP)']]
    insert_LP_pesquisador(conn,df_pesquisadores_LPs)
    return


########################################################################################################################
######   MAIN

print("Starting connection..." )
conn = connectDB()
print("Loading data from Google Sheet..." )
df_datasheet = connect2Data()

erase_data(conn)

# print("Inserting Núcleos de pesquisa..." )
insertNucleos(conn)

# print("Inserting Linhas de pesquisa..." )
insertLPs(conn, df_datasheet)

# print("Inserting Universidades e campi..." )
insertCampi(conn,df_datasheet)

# print("Inserting Usuários, Pesquisadores..." )
loadUsuarios(conn,df_datasheet)



# conda activate pescarte-import
# python -W ignore import_script.py      OU
# python import_script.py
# servidor SMTP - de email: python -m smtpd -c DebuggingServer -n localhost:1025

# Para mexer no script de importação de pesquisadores e atualizar no GITHUB
# git add import_script.py
# git commit -m "some improvements" (possivelmente “atualizando enderecos”)
# git push



### TO DO
# verificar linhas do IF 571-574
# catch exception
# vacuum from python
# OK deixar o endereco como esta na planilha, apenas separar a cidade - manter endereco num campo so, ignorar rua e complemento
# OK verificar o esquema real
# email real do noreply-pea-pescarte@uenf.br  para testar/enviar! : hoje vai copia do email para o sender, tem como evitar?
# preciso da descricao longa dos nucleos e LPs
# OK testado! email
# OK tem 104 insercoes ao inves de 106 - revisar os enderecos com cep nao encontrado e colocar o original
# OK rowcount
# OK fazer o mandar email quando a senha eh criada
# OK arrumar role para pesquisador
# OK limpar telefone sem ()



# verificado!!!
# - formato do telefone sem (DDD)? so numeros OK
# - RG: passar para o usuario OK
# - sem genero OK
# - contact_id em user desnecessario assim como pesquisador_id: deixa como está!
# - numero e complemento do endereco no mesmo campo e nao separado
# -- usei o complemento para colocar bairro 
# -- o endereco dos campi esta inserido errado: vamos deixar tudo no campo endereco como está
# -- tem ceps q nao existem mas a Livia falou q era para deixar OK
# - nome de tabela user: Zoey esta ciente

# - data de termino vs data fim bolsa - apagar (nao usar) data_termino OK
# - inserir campo OBSERVACOES/ANOTACOES OK ver com Anthony a interface do admin de pesquisa
# - nao fiz a parte de orientacao como combinado OK - ate eles estao confusos e a Gisele pode fazer dps
# - send email so de teste: pea-pescarte@uenf.br, noreply-pea-pescarte@uenf.br
# - link da foto no drive ou blob no pg? link mesmo!
# - linkedin nao esta na planilha - cada pesquisador pode fazer dps (opcional)



# verificado!!!
# select * from pesquisador
# where data_inicio_bolsa > data_fim_bolsa or data_inicio_bolsa < data_contratacao




