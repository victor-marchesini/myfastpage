# -*- coding: utf-8 -*-
import os.path
import pandas as pd
import ssl
from urllib.request import urlopen
from bs4 import BeautifulSoup
import altair as alt

def get_last_bd_srag_csv_url(year=2021):
    
    available_years = (2019,2020,2021)
    if year not in available_years:
        print('year not valid. Available years:',available_years)
        return
    
    # Se nao achar, retorna última url encontrada
    srag_url = f'https://s3-sa-east-1.amazonaws.com/ckan.saude.gov.br/SRAG/{year}/INFLUD-29-03-2021.csv'
    
    context = ssl._create_unverified_context() # To aviod ssl error
    bd_srag_url = f'https://opendatasus.saude.gov.br/dataset/bd-srag-{year}'
    html_page = urlopen(bd_srag_url, context=context)
    soup = BeautifulSoup(html_page, features="lxml")
    for link in soup.findAll('a'):
        url = link.get('href')
        (filename, ext) = os.path.splitext(url)
        if ext.lower() == '.csv':
            srag_url = url
            print(f'\nCsv file found at <{bd_srag_url}>')
    
    return srag_url

def get_srag_data(years=[2021],update=True,save_local=True,treat=True,selected_columns='BASIC',aditional_columns=[]):
    
    sep = ';'
    quotechar = '"'
    frames = []
    for year in years:
        fname = f'data/opendatasus/INFLUD{year}.csv'
        if os.path.isfile(fname) and not update:
            print(f'\nReading OpenDataSus from local file <{fname}>. If you prefer to download last version, set "update=True".\n')
            df = pd.read_csv(fname,dtype=object)
        else:
            url = get_last_bd_srag_csv_url(year)
            print(f'\nDownloading from <{url}> ... ', end='')
            df = pd.read_csv(url,sep=sep,quotechar=quotechar,dtype=object, encoding='latin1')
            if save_local:
                df.to_csv(fname,index=False)
            print('complete!\n')
        frames.append(df)
    
    df = pd.concat(frames)
    if treat:
        df = treat_srag_data(df,selected_columns,aditional_columns)
    
    return df

def get_cities_states_dictionaries():
    ''' Returns 2 dictionaries: 
    1. cities_dict - city code (6 dig): city name
    2. states_dict - state code (2 dig): state name
    '''
    fname = 'data/IBGE/RELATORIO_DTB_BRASIL_MUNICIPIO.ods'
    df = pd.read_excel(fname,dtype=object)
    df['cod_municipio'] = df['Código Município Completo'].str[:6]
    cities_dict = df.set_index('cod_municipio')['Nome_Município'].to_dict()
    states_dict = df[['UF','Nome_UF']].groupby('UF').first()['Nome_UF'].to_dict()
    return cities_dict, states_dict

def set_age_ranges(x):
    if x <= 20:
        return '00-20'
    elif x <= 40:
        return '20-40'
    elif x <= 60:
        return '40-60'
    elif x <= 70:
        return '60-70'
    elif x <= 80:
        return '70-80'
    else:
        return '80+'

def treat_srag_data(df_orig,selected_columns='',aditional_columns=[]):
    "Select columns, set types and replace values."
    
    not_valid_col = 'nd'
    
    df = df_orig.copy()
    date_cols = ['DT_SIN_PRI','DT_EVOLUCA','DT_NASC','DT_ENTUTI']
    cities_cols = ['CO_MUN_RES','CO_MU_INTE','CO_MUN_NOT']
    uf_cols = ['SG_UF','SG_UF_NOT','SG_UF_INTE']
    
    if selected_columns != 'ALL':
        basic_cols = date_cols + cities_cols + uf_cols
        basic_cols += ['SEM_PRI', 'EVOLUCAO', 'CLASSI_FIN','CLASSI_OUT',                
                      'NU_IDADE_N','CS_RACA', 'CS_ESCOL_N', 'CS_SEXO',
                      #'ID_MN_RESI','ID_MN_ITE','ID_MUNICIP',
                      'SUPORT_VEN', 'UTI','SATURACAO','FATOR_RISC']
    
        if selected_columns == 'BASIC' or not aditional_columns:
            cols = basic_cols
        else:
            if type(aditional_columns) is list:
                cols = basic_cols + aditional_columns
            else:
                print('O parâmetro <aditional_columns> deve ser do tipo list')
            
        orig_cols = df_orig.columns
        for col in cols:
            if col not in orig_cols:
                df[col] = not_valid_col
        
        df = df[cols]
        
    df_cols = df.columns
    numeric_cols = ['SEM_PRI','NU_IDADE_N']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce',dayfirst=True)

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
    # add age_col 'idade_anos'
    age_col = 'dv_idade_anos'
    df[age_col] = (df.DT_SIN_PRI - df.DT_NASC).dt.days / 365.25
    mask = df[age_col].isna()
    div_tipo = {'1':365.25,'2':12.,'3':1.}
    df.loc[mask,age_col] = df_orig.loc[mask,'NU_IDADE_N'].astype(int) / df_orig.loc[mask,'TP_IDADE'].apply(lambda x: div_tipo.get(x,'n.d.'))
    
    df['dv_faixa_etaria'] = df[age_col].apply(set_age_ranges)
    
    first_date_2021 = pd.to_datetime('2021-01-03')
    mask = (df.DT_SIN_PRI >= first_date_2021)
    df['dv_SEM_PRI_ABS'] = df['SEM_PRI']
    df.loc[mask,'dv_SEM_PRI_ABS'] = df.loc[mask,'SEM_PRI'] + 53
    
    cities_dict,states_dict = get_cities_states_dictionaries()
    regions_dict = {'1':'1_Norte',
                    '2':'2_Nordeste',
                    '3':'3_Sudeste',
                    '4':'4_Sul',
                    '5':'5_Centro-Oeste' }
    
    
    for col in cities_cols:
        sufix_index = col.index('_',3)
        city_name_col = 'dv_MUN' + col[sufix_index:]
        state_name_col = 'dv_UF' + col[sufix_index:]
        region_name_col = 'dv_REGIAO' + col[sufix_index:]
        df[city_name_col] = df[col].apply(lambda x: cities_dict.get(x,not_valid_col))
        df[state_name_col] = df[col].str[:2].apply(lambda x: '{}_{}'.format(x,states_dict.get(x,not_valid_col)))
        df[region_name_col] = df[col].str[0].apply(lambda x: regions_dict.get(x,not_valid_col))
        
    evolucao_dict = {'1':'cura',
                     '2':'obito',
                     '3':'obito_outras_causas',
                     '9':'ignorado' }
    classi_fin_dict = {'1':'Influenza',
                       '2':'outro vírus respiratório',
                       '3':'ooutro agente etiológico',
                       '4':'não especificado',
                       '5':'COVID-19'}
    
    raca_dict = {'1':'branca',
                 '2':'preta',
                 '3':'amarela',
                 '4':'parda',
                 '5':'indigena',
                 '9':'ignorado' }
    escol_dict = {'0':'0_sem_escolaridade/analfabeto',
                  '1':'1_fundamental_1',
                  '2':'2_fundamental_2',
                  '3':'3_medio',
                  '4':'4_superior',
                  '5':'nao_se_aplica',
                  '9':'ignorado'
                 }
    evolucao_dict = {'1':'cura',
                     '2':'obito',
                     '3':'obito_outras_causas',
                     '9':'ignorado' }
    suport_dict = {'1':'1_sim | invasivo',
                   '2':'2_sim | nao_invasivo',
                   '3':'3_nao',
                   '9':'ignorado' }
    basic_dict = {'1':'sim',
                  '2':'nao',
                  '9':'ignorado' }
    basic_dict_sn = {'S':'sim',
                     'N':'nao' }


    df['EVOLUCAO'] = df['EVOLUCAO'].apply(lambda x: evolucao_dict.get(x,not_valid_col))
    df['CLASSI_FIN'] = df['CLASSI_FIN'].apply(lambda x: classi_fin_dict.get(x,not_valid_col))
    df['CS_RACA'] = df['CS_RACA'].apply(lambda x: raca_dict.get(x,not_valid_col))
    df['CS_ESCOL_N'] = df['CS_ESCOL_N'].apply(lambda x: escol_dict.get(x,not_valid_col))
    df['SUPORT_VEN'] = df['SUPORT_VEN'].apply(lambda x: suport_dict.get(x,not_valid_col))
    df['UTI'] = df['UTI'].apply(lambda x: basic_dict.get(x,not_valid_col))
    df['SATURACAO'] = df['SATURACAO'].apply(lambda x: basic_dict.get(x,not_valid_col))
    df['FATOR_RISC'] = df['FATOR_RISC'].apply(lambda x: basic_dict_sn.get(x,not_valid_col))
    
    dict_cols = ['EVOLUCAO','CLASSI_FIN','CS_RACA','CS_ESCOL_N','SUPORT_VEN','UTI','SATURACAO','FATOR_RISC']
    
    other_cols = list(set(df_cols) - set(date_cols) - set(numeric_cols) - set(cities_cols) - set(dict_cols))
    df[other_cols] = df[other_cols].fillna(not_valid_col)
        
    return df

def get_pivot_data(df,index_cols=[],columns_cols=[],values_cols='',last_week=999,total=True):
    
    df = df.groupby(by=index_cols + columns_cols)[values_cols].count().reset_index()
    df = df.pivot(index=index_cols,columns=columns_cols,values=values_cols).fillna(0)
    n_index = len(index_cols)
    if n_index > 1:
        if type(total) is bool:
            total = [total]* (n_index - 1)
        for i in range(1,n_index):
            if total[i-1]:
                df1 = df.reset_index().set_index(index_cols[:i])
                df2 = df1.groupby(by=index_cols[:i]).sum()
                df2[index_cols[i:]] = '--TODOS--'
                df = pd.concat([df1,df2])
        
    df['total'] = df.sum(axis=1)
    df = df.reset_index()
    return df

def select_items(df,selection_dict):
    df = df.copy()
    for column,value in selection_dict.items():
        df = df.query(f'{column}{value}')
    return df

def get_outcome_data(df,index_cols,total=True,rates=True):
    
    if len(index_cols) > 1:
        new_index = index_cols[:1]
        for col in index_cols[1:]:
            new_col = col
            if col in new_index:
                new_col = '_' + col
                df[new_col] =  df[col]
            new_index.append(new_col)
        index_cols = new_index
    
    columns_cols = ['EVOLUCAO']
    values_cols = 'DT_SIN_PRI'
    df = get_pivot_data(df,index_cols,columns_cols,values_cols,total=total)
    
    if rates:
        total_obitos = df['obito'] #+ df['obito_outras_causas']
        total_concluidos = df['obito'] + df['cura'] #+ df['obito_outras_causas']
        df['tx_obito_andamento'] = total_obitos / df['total']
        df['tx_obito_concluido'] = total_obitos / total_concluidos
    return df

def get_altair_chart_2_axis(df, x_col, cat_col, y_cols,chart_title=''):
    
    y_col_1 = y_cols[0]
    y_col_2 = y_cols[1]

    options_list = df[cat_col].unique().tolist()
    selection = alt.selection_single(
        name='Selecione',
        fields=[cat_col],
        init={cat_col: options_list[0]},
        bind={cat_col: alt.binding_select(options=options_list)}
    )
    
    base = alt.Chart(df).encode(
        alt.X(x_col, axis=alt.Axis(title='Semana Primeiros sintomas'))
    )
    line1 = base.mark_line(stroke='#57A44C', interpolate='monotone').add_selection(
        selection
    ).encode(alt.Y(y_col_1,axis=alt.Axis(title='Taxa de óbito', titleColor='#57A44C'))
             ,color=cat_col
             ,tooltip=list(df.columns)
            ).transform_filter(
        selection
    )
    
    line2 = base.mark_line(stroke='#5276A7', interpolate='monotone').encode(
        alt.Y(y_col_2,axis=alt.Axis(title='Total de casos', titleColor='#5276A7'))
        ,color=cat_col
        ,tooltip=list(df.columns)
    ).transform_filter(
        selection
    )
    
    chart = alt.layer(line1, line2).resolve_scale(
        y = 'independent'
    ).properties(
        width=800,
        height=500,
        title=chart_title
    )
    return chart


def get_altair_chart(df,x_col,y_cols='ALL',cat_col=None,sel_cols=None, sliders=None, ns_opacity=1.0,chart_title='',scheme = 'lightmulti',mark_type='line',sort_values=False,y_index = -1,stack=None):

    if mark_type == 'bar':
        chart = alt.Chart(df).mark_bar() 
    elif mark_type == 'area':
        chart = alt.Chart(df).mark_area() 
    else:
        chart = alt.Chart(df).mark_line(point=True,strokeWidth=2) 
    
    sort_axis = 'x'
    x_col_ed = x_col
    if sort_values:
        x_col_ed=alt.X(f'{x_col}:N', sort='y')
   
    chart = chart.encode(
        x=x_col_ed,
        tooltip=list(df.columns),
    ).properties(
        width=600,
        height=400
    )#.interactive()
    

    if sliders:
        for key,value in sliders.items():
            if key == 'min':
                comparisson = '>='
            elif key == 'max':
                comparisson = '<='
            else:
                print(f"Atenção: a chave '{key}' não é válida para a variável sliders. Usar apenas 'min' ou 'max'")
                
                continue
            if type(value) is list:
                slider_col = value[0]
                if len(value) > 1:
                    init_value = value[1]
                else:
                    init_value = eval(f'{key}(df[slider_col])')
            else:   
                slider_col = value
                init_value = eval(f'{key}(df[slider_col])')

            if slider_col in df.columns:
                slider = alt.binding_range(min=min(df[slider_col]), max=max(df[slider_col]), step=1)
                slider_selector = alt.selection_single(bind=slider,name=key, fields=[slider_col],
                                                       init={slider_col: init_value}
                                                      )
                chart = chart.add_selection(slider_selector).transform_filter(f'datum.{slider_col} {comparisson} {key}.{slider_col}[0]')

           

    
    if y_cols =='ALL':
        index = 1
        if cat_col:
            index += 1
        if sel_cols:
            index += len(sel_cols)
            
        y_cols = df.columns[index:].to_list()
        
    if len(y_cols) > 1:
        columns = y_cols
        y_col_name = 'Y_col'
        select_box = alt.binding_select(options=columns, name=y_col_name)
        sel = alt.selection_single(fields=[y_col_name], bind=select_box, init={y_col_name: y_cols[y_index]})
        
        chart = chart.transform_fold(
            columns,
            as_=[y_col_name, 'Valor']
        ).transform_filter(
            sel  
        )
        if stack == 'normalize':
            chart = chart.encode(
                y=alt.Y("Valor:Q", stack="normalize"),
            )
        elif stack == 'sum':
            chart = chart.encode(
                y='sum(Valor):Q',
            )
        else:
            chart = chart.encode(
                y='Valor:Q',
             )
        chart = chart.add_selection(sel)
    else:
        y_col = y_cols[0]
        chart = chart.encode(
            y=y_col
        )        

#     TODO: adicionar filtro de range
#     lower = chart.properties(
#         height=60
#     ).add_selection(brush)
#     chart = chart & lower

    if cat_col:
        base_cat = cat_col        
        chart = chart.encode(
            color=alt.Color(base_cat, scale=alt.Scale(scheme=scheme)), #,legend=None),
        )
    
        sel_base = alt.selection_multi(empty='all', fields=[base_cat], bind='legend')
    
        chart = chart.add_selection(
            sel_base
        ).encode(
            opacity=alt.condition(sel_base, alt.value(1.0), alt.value(ns_opacity))
        )

        bar = alt.Chart(df).mark_bar().encode(
            y=alt.Y(f'{base_cat}:O',title=None),
            x='total',
#             tooltip='total',
            color=alt.condition(sel_base, alt.Color(f'{base_cat}:N', scale=alt.Scale(scheme=scheme)), alt.ColorValue("lightgrey"),legend=None)
        ).add_selection(sel_base).properties(
            width=100,
            height=400
        )

        chart = alt.concat(
                chart,
                bar
        )

#         chart = chart & lower  TODO: adicionar fltro de range

    select_cols = sel_cols
    if select_cols:

        options_lists = [df[cat].dropna().astype(str).sort_values().unique().tolist() for cat in select_cols]

        selection = alt.selection_single(
                name='Selecione',
                fields=select_cols,
                init={cat: options_lists[i][0] for i,cat in enumerate(select_cols)},
                bind={cat: alt.binding_select(options=options_lists[i]) for i,cat in enumerate(select_cols)}
            )

        chart = chart.add_selection(
                selection
            ).transform_filter(
                selection
            )
    
    return chart


def dataFrame2Chart(df,x_col,cat_col=None,sel_cols=None,selection_dict={},sliders=None,y_cols='ALL',chart_title='',ns_opacity=0.1,
                    scheme ='lightmulti',mark_type='line',sort_values=False,naxis=1,total=True,stack=None,rates=True):
    print(f'Seleção {chart_title}:')
    for key,value in selection_dict.items():
        print(f'\t{key} {value}')
    df_sel = select_items(df,selection_dict)
    print('\t-----\n\tNúmero de casos selecionados: {} ({:.2%} do total de casos disponíveis.)\n'.format(df_sel.shape[0], df_sel.shape[0] / df.shape[0]))
    
    index_cols = [x_col]
    if cat_col:
        index_cols += [cat_col]
    if sel_cols:
        index_cols += sel_cols

    df = get_outcome_data(df_sel,index_cols,total=total,rates=rates)
    print('\tDimensões dos dados do gráfico:', df.shape)
    if naxis == 1:
        chart = get_altair_chart(df,x_col,y_cols,cat_col,sel_cols,sliders=sliders,ns_opacity=ns_opacity,
                                 chart_title=chart_title, scheme=scheme,mark_type=mark_type,sort_values=sort_values,stack=stack)
    elif naxis == 2:
        chart = get_altair_chart_2_axis(df, x_col=index_cols[0], cat_col=index_cols[1], y_cols=y_cols,chart_title=chart_title)
    else:
        print('Não implementado ainda.')
    return chart

# sliders = {'min':['SEM_PRI_ABS',10],
#            'max':['SEM_PRI_ABS',50]}
# dataFrame2Chart(df_srag,x_col,cat_col,sel_cols,selection_dict,sliders=sliders,total=True)
