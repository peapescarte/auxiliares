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


# credenciais BD
HOST = "localhost"
PORT = "5432"
DATABASE = "pescarte_import"
USER = "sahudymontenegro"
PASSWORD = "xxx"


########################################################################################################################
#                     AUXILIARES

def generate_nanoid(size, len=21) :
    return [generate(size=len) for i in range(1,size)]

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
            endereco = get_address_from_cep(cep, webservice=ws)
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
        if u[5] == 'S' :    #  'ATIVO (S/N)'
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
        print(error)
        if conn is not None:
            conn.close()
            print('Database connection closed.')

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
    sqltxt = "INSERT INTO nucleo_pesquisa VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"

    vals = (int('1'),'CULTURA', 'NUCLEO A - CULTURA', int('1'), 'A')
    cur.execute(sqltxt,vals)

    vals = ('2', 'RECURSOS HÍDRICOS E ALIMENTARES', 'NUCLEO B - RECURSOS HÍDRICOS E ALIMENTARES', '2', 'B')
    cur.execute(sqltxt,vals)

    vals = ('3','SOCIABILIDADES E PARTICIPAÇÃO', 'NUCLEO C - SOCIABILIDADES E PARTICIPAÇÃO', '3', 'C')
    cur.execute(sqltxt,vals)

    vals = ('4','CENSO E AFINS', 'NUCLEO D - CENSO E AFINS', '4', 'D')
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

    engine = getEngine()
    # achar o id do nucleo de pesquisa
    with engine.connect() as connection:
        sql_query = text("""select id, letter from nucleo_pesquisa""")
        result = connection.execute(sql_query)

    df_nucleos = pd.DataFrame(result, columns = ['nucleo_pesquisa_id', 'nucleo'])

    df_linhaspesquisa = df_linhaspesquisa.join(df_nucleos.set_index('nucleo'), how='left', on='nucleo')
    df_linhaspesquisa = df_linhaspesquisa[['number','short_desc','nucleo_pesquisa_id']]

    # atualizar o id da linha de pesquisa com o NoLP (que vem do dataframe index)
    df_linhaspesquisa['id'] = df_linhaspesquisa.loc[:, 'number']
    # public_id - usando nanoid size=10 https://pypi.org/project/nanoid/
    nanoids = generate_nanoid(len(df_linhaspesquisa.index)+1)
    df_linhaspesquisa.insert(1,"public_id",nanoids, allow_duplicates=False)

    # importando para o BD - tabela linha_pesquisa
    # to_sql do Dataframe insere um dataframe completo na tabela
    # como nao da para fazer upsert usando to_sql, verifico se a tabela está populada 
 
    # Opcao 2: (como é um script de uma única vez suponho que se existem tuplas inseridas, posso apagar e reinserir)
    empty_table(conn,"linha_pesquisa")
  
    # Opcao 1: (como é um script de uma única vez suponho que se existem tuplas inseridas, elas são as corretas)
    # if records[0][0] == 0 :
    #     df_linhaspesquisa.to_sql('linha_pesquisa', con=engine, if_exists='append', index=False)   #, index_label='public_id')
    #     print("LPs inserted")
    # else :
    #     print("LPs: nothing to do here...")

    df_linhaspesquisa.to_sql('linha_pesquisa', con=engine, if_exists='append', index=False)  
    print("LPs inserted")


########################################################################################################################
#                     AUXILIARES SQLs

def insertCounty(conn,df_county) :
    df_county = df_county.drop_duplicates().dropna()
    nanoids = generate_nanoid(len(df_county.index)+1)
    df_county.insert(0,"public_id",nanoids, allow_duplicates=False)

    # ON CONFLICT deveria ser (county,uf) se fosse generico mas nao funciona pq nao ha PK ou UNIQUE ou INDEX em county
    sqltxt = "INSERT INTO city (id, public_id, county, uf) VALUES (%s, %s, %s, %s)" # ON CONFLICT (county) DO NOTHING"

    cur = conn.cursor()
    for idx,cidade in df_county.iterrows() :
        if not find_city(conn,cidade['county']):
            last_id = get_lastID(conn,'city')
            vals = (last_id + 1, cidade['public_id'], cidade['county'].lower(), cidade['uf'].lower())
            cur.execute(sqltxt,vals)

    conn.commit()
    cur.close()
    print("Cidades + UFs inserted")
    return df_county

def find_city(conn,cidade) :
    cur = conn.cursor()
    sqltxt = "SELECT * FROM city WHERE county ILIKE '%s'" % cidade.lower()
    cur.execute(sqltxt)
    if cur.rowcount == 0 :
        cur.close()
        return False
    cur.close()
    return True

def get_lastID(conn,table) :
    cur = conn.cursor()
    sqltxt = 'SELECT max(id) FROM %s' % table
    cur.execute(sqltxt)
    max = cur.fetchone()
    cur.close()
    if max[0] == None :
        return 0
    return max[0]

def get_cityID(conn, cidade) :
    cur = conn.cursor()
    sqltxt = "SELECT id FROM city WHERE county ILIKE '%s'" % cidade.lower()
    cur.execute(sqltxt)
    city_id = cur.fetchone()
    if city_id == None :
        cur.close()
        return 0
    cur.close()
    return city_id[0]

def get_UF(conn, cidade) :
    cur = conn.cursor()
    sqltxt = "SELECT uf FROM city WHERE county ILIKE '%s'" % cidade.lower()
    cur.execute(sqltxt)
    uf = cur.fetchone()
    if uf == None :
        cur.close()
        return 0
    cur.close()
    return uf[0]

def get_userID(conn, cpf) :
    cur = conn.cursor()
    sqltxt = 'SELECT id FROM public."user" ' + "WHERE cpf ILIKE '%s'" % cpf
    cur.execute(sqltxt)
    user_id = cur.fetchone()
    if user_id == None :
        cur.close()
        return 0
    cur.close()
    return user_id[0]

def get_campusID(conn, univ_sigla, campus) :
    cur = conn.cursor()
    sqltxt = "SELECT id FROM campi WHERE initials ILIKE '%s' AND name ILIKE '%s'" % (univ_sigla, campus)
    cur.execute(sqltxt)
    campus_id = cur.fetchone()
    cur.close()
    if campus_id == None:
        return None
    return campus_id[0]

def get_pesquisadorID(conn,lattes) :
    cur = conn.cursor()
    sqltxt = "SELECT id FROM pesquisador WHERE link_lattes ILIKE '%s'" % lattes
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
    empty_table(conn,'"public".user')
    empty_table(conn,"contato")
    empty_table(conn,"campi")
    empty_table(conn,"address")
    empty_table(conn,"city")

    # vacuum(conn)
    print("ADDRESS + CONTACT + User + Pesquisador e suas LPs => ALL DELETED")

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

    lst = [i for i in range(1,len(df_universidades_core.index)+1)]
    df_universidades_core.insert(0,"id",lst, allow_duplicates=False) #NAO PRECISA, É SERIAL NO BD!
    # acrescentando o id public como nanoid
    nanoids = generate_nanoid(len(df_universidades_core.index)+1)
    df_universidades_core.insert(1,"public_id",nanoids, allow_duplicates=False)

    sqltxt = "INSERT INTO campi (id, public_id, initials, name, university_name, address_id) VALUES (%s, %s, %s, %s, %s, %s)" 

    cur = conn.cursor()
    for idx,campus in df_universidades_core.iterrows() :
        address_id = df_universidades_address.at[idx,"id"]
        vals = (campus["id"], campus['public_id'], campus['initials'], campus['name'], campus['university_name'], int(address_id))
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

    insertCounty(conn,df_address[['county','uf']])

    lst = [i for i in range(1,len(df_address.index)+1)]
    df_address.insert(0,"id",lst, allow_duplicates=False)

    sqltxt_address = "INSERT INTO address (id, rua, cep, city_id) VALUES (%s, %s, %s, %s)"

    cur = conn.cursor()
    for idx, ad in df_address.iterrows() :
        city_id = get_cityID(conn,ad['county'])
        last_address_id = get_lastID(conn,'address')
        vals_address = (last_address_id+1, ad['rua'], ad['CEP - CAMPUS'], city_id)
        cur.execute(sqltxt_address, vals_address)

    conn.commit()
    cur.close()
    print("Addresses inserted")
    return df_address


########################################################################################################################
#                     USUARIOS     +     PESQUISADORES

def insertContactInfo(conn,df_contato) :
    cur = conn.cursor()

    sqltxt_address = "INSERT INTO address (id, rua, cep, city_id) VALUES (%s, %s, %s, %s)"
    sqltxt_contato = "INSERT INTO contato (id, mobile_principal, email_principal, mobile_outros, email_outros, address) VALUES (%s, %s, %s, %s, %s, %s)"

    for idx,contact in df_contato.iterrows() :
        # inserir o endereco
        city_id = get_cityID(conn,contact['county'])
        last_address_id = get_lastID(conn,'address')
        vals_address = (last_address_id+1, contact['Endereço'], contact['CEP'], city_id)
        cur.execute(sqltxt_address, vals_address)

        # inserir contato com FK do endereco
        if contact['Outros telefones'] == '{None}' :
            contact['Outros telefones'] = None
        if contact['Outros E-mails'] == '{None}' :
            contact['Outros E-mails'] = None
        vals_contato = (int(contact['id']), contact['Telefone'], contact['E-mail'], contact['Outros telefones'], contact['Outros E-mails'], last_address_id+1)
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
    sqltxt = 'INSERT INTO public."user" (id, public_id, cpf, rg, first_name, middle_name, last_name, birthdate, role, password_hash, avatar_link, contato_id) \
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

    for idx,u in df_usuarios.iterrows() :
        vals = (int(u['id']), u['public_id'], u['CPF'], u['RG'], \
            u['BOLSISTA - PRIMEIRO NOME'], u['BOLSISTA - NOME DO MEIO'], u['BOLSISTA - ÚLTIMO NOME'], \
            u['Data de Nascimento'], "pesquisador", u['Hashed'], u['Foto'], int(u['id']))
        cur.execute(sqltxt, vals)

    conn.commit()
    cur.close()
    print("User's core data inserted")

def insertDadosPesquisa(conn,df_pesquisa) :
    cur = conn.cursor()

    # garantindo formato de DATA DMY
    sql = "SET datestyle to DMY, SQL; "
    cur.execute(sql)
    sqltxt = 'INSERT INTO pesquisador (id, public_id, bolsa, link_lattes, formacao, \
          data_inicio_bolsa, data_fim_bolsa, data_contratacao, ativo, campus_id, user_id) \
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

    for idx,u in df_pesquisa.iterrows() :
        campus_id = get_campusID(conn,u['UNIVERSIDADE SIGLA'], u['CAMPUS']) 
        pesquisador_id = get_userID(conn,u['CPF'])
        vals = (pesquisador_id, u['public_id'], u['TIPO DE BOLSA'], \
                u['LINK LATTES'], u['FORMAÇÃO'], \
                    u['Data de início BOLSA'], u['Data de fim BOLSA'], u['DATA DA CONTRATAÇÃO'], \
                    u['ATIVO (S/N)'], campus_id, pesquisador_id)
        cur.execute(sqltxt, vals)

    conn.commit()
    cur.close()
    print("Dados específicos da pesquisa inserted")

def insert_LP_pesquisador(conn,df_LP):
    cur = conn.cursor()
    sqltxt = 'INSERT INTO "pesquisador_LP" (pesquisador, linha_pesquisa, lider) \
                VALUES (%s, %s, %s)'
    for idx,lp in df_LP.iterrows() :
        pesquisador_id = get_pesquisadorID(conn,lp['LINK LATTES'])
        lider = False
        if lp['Responsavel pela LP? (insira o numero da LP)'] == lp['NoLP'] :
            lider = True
        vals = (pesquisador_id, lp['NoLP'],lider)
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
    # address_OK, bairro = split_stringlist(address,'bairro')
    # df_usuarios_contato.insert(2,'Bairro',bairro)  
    # bairro_OK, cidade = split_stringlist(bairro,'cidade')
    # df_usuarios_contato['Bairro'] = bairro_OK
    df_usuarios_contato.insert(3,'Cidade',cidade)

    # juntar telef e email de varias linhas do mesmo pesquisador
    df_usuarios_contato.update(pd.DataFrame(df_usuarios_contato['Telefone'].apply(lambda row: add_separator(row,' (',';('))))
    df_usuarios_contato.update(pd.DataFrame(df_usuarios_contato['E-mail'].apply(lambda row: add_separator(row,',',';'))))

    df_usuarios_contato.drop_duplicates(inplace=True,keep='first')

    # separando em dois campos: first|rest
    # (telefone,outros_telefones) 
    # df_usuarios_contato.update(pd.DataFrame(df_usuarios_contato['Telefone'].astype(str).apply(lambda row: remove_duplicates(row))))
    df_usuarios_contato.update(pd.DataFrame(df_usuarios_contato['Telefone'].astype(str).apply(lambda row: trim_special_chars(row,chars=['.','-',' ','(',')'])))) #  outros possiveis a remover
    df_usuarios_contato[['Telefone','Outros telefones']] = pd.DataFrame(df_usuarios_contato['Telefone']\
                .str.split(';', n=1, expand=True, regex=False)\
                .applymap(lambda value: None if value == "" else value),     
                index= df_usuarios_contato.index) # `.applymap` converts empty strings ("") into None.
    df_usuarios_contato.update(pd.DataFrame(df_usuarios_contato['Outros telefones'].astype(str).apply(lambda row: "{" + row + "}")))
    # (email,outros_emails) 
    df_usuarios_contato.update(pd.DataFrame(df_usuarios_contato['E-mail'].astype(str).apply(lambda row: trim_special_chars(row,chars=[' ']))))
    df_usuarios_contato[['E-mail','Outros E-mails']] = pd.DataFrame(df_usuarios_contato['E-mail'].str.split(';', n=1, expand=True, regex=False), index= df_usuarios_contato.index)
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
            uf = get_UF(conn,df_address.at[idx,'county']) #df_usuarios_contato.at[idx,'UF'] # nao tem UF da universidade na planilha, os 2 casos em q  cep nao da certo sao de RJ
            if uf != 0 :
                df_address.at[idx,'uf'] =  uf  
            else :
                df_address.at[idx,'uf'] = 'RJ' # nao tem UF da universidade na planilha, os 2 casos em q  cep nao da certo sao de RJ

    # insere cidades+uf do endereço dos pequisadores
    insertCounty(conn,df_address[['county','uf']])
    df_usuarios_contato = df_usuarios_contato.join(df_address.set_index('CEP'), on='CEP', how='left')

    # acrescentando o id unico sequencial da tabela
    # controlando o id no codigo pois serve de chave estrangeira para a tabela de endereços
    lst = [i for i in range(1,len(df_usuarios_contato.index)+1)]
    df_usuarios_contato.insert(0,"id",lst, allow_duplicates=False)

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
    insertContactInfo(conn,df_usuarios_contato[['id', 'Endereço', 'CEP', 'Telefone',\
        'E-mail', 'Outros telefones', 'Outros E-mails', 'county']]) # em casos mais genericos precisa passar o 'uf' para conferir unicidade de cidade

    # inserindo o restante dos dados na tabela de usuarios com perfil pesquisador
    df_usuarios = df_sheet[['CPF', 'RG', 'BOLSISTA - PRIMEIRO NOME', \
        'BOLSISTA - NOME DO MEIO', 'BOLSISTA - ÚLTIMO NOME', \
        'ATIVO (S/N)', # apenas para saber se mandar email
        'Data de Nascimento', 'Foto', 'Telefone']]
    df_usuarios.update(pd.DataFrame(df_usuarios['CPF']\
        .apply(lambda row: trim_special_chars(row)).astype(str))) 
    df_usuarios.update(pd.DataFrame(df_usuarios['RG'].astype(str)\
        .apply(lambda row: trim_special_chars(row)).astype(str))) 

    # removendo duplicatas que tinham CPF com caracteres especiais diferentes
    df_usuarios.drop_duplicates(inplace = True)
    # inserir id do contato para usar no usuario e o email para mandar email com a senha de usuario
    df_usuarios = df_usuarios.join(df_usuarios_contato[['id', 'E-mail']],how='inner')

    # gerando public_id do usuario
    nanoids = generate_nanoid(len(df_usuarios.index)+1)
    df_usuarios.insert(0,"public_id",nanoids, allow_duplicates=False)

    # criando passwords para users
    senhas = generate_nanoid(len(df_usuarios.index)+1, len = 8)
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
                                     'LINK LATTES', 'UNIVERSIDADE SIGLA', 'CAMPUS', 'ATIVO (S/N)', \
                                     'Data de início BOLSA', 'Data de fim BOLSA']]
    df_pesquisadores.update(pd.DataFrame(df_pesquisadores['CPF']\
        .apply(lambda row: trim_special_chars(row)).astype(str))) 
    df_pesquisadores.drop_duplicates(inplace=True,keep='first')
    # gerando public_id do pesquisador
    nanoids = generate_nanoid(len(df_pesquisadores.index)+1)
    df_pesquisadores.insert(0,"public_id",nanoids, allow_duplicates=False)

    df_pesquisadores['Data de fim BOLSA'].where(~df_pesquisadores['Data de fim BOLSA'].isna(), None, inplace=True)
    df_pesquisadores['ATIVO (S/N)'].mask(df_pesquisadores['ATIVO (S/N)']=='S', other=True, inplace=True)
    df_pesquisadores['ATIVO (S/N)'].mask(df_pesquisadores['ATIVO (S/N)']=='N', other=False, inplace=True)

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

# print("Inserting Núcleos de pesquisa..." )
insertNucleos(conn)

# print("Inserting Linhas de pesquisa..." )
insertLPs(conn, df_datasheet)

erase_data(conn)

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
# catch exception
# vacuum from python
# OK deixar o endereco como esta na planilha, apenas separar a cidade - manter endereco num campo so, ignorar rua e complemento
# verificar o esquema real
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




