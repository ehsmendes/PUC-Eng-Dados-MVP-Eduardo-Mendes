## PUC-Eng-Dados-MVP
### MVP de Engenharia de dados PUC
[Clique aqui para acessar o Notebook com os códigos, resultado de queries e gráficos](
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2149961212973897/667217794031682/3413641925918938/latest.html)

#### 1 - Objetivo: Objetivo.
De janeiro de 2020 até dezembro de 2021 causou uma revolução mundial devido à pandemia de COVID-19, em pouco tempo, vimos mudanças drásticas na mudança de hábito, medo, morte e instabilidade econômica. O que antes era considerado normal, em um piscar de olhos se tornou perigoso, vivíamos a era do "novo normal". 
Este tema tão intrigante, devido a estes e outros fatores, despertou a minha curiosidade por isso o elegi para, pelo menos tentar responder com dados, algumas perguntas que pairaram na minha mente neste período. 

Além de tentar me ajudar com estas dúvidas o trabalho tem como objetivo entender como ficou a distribuição dos casos reportados (Brasil e mundo), distribuição da taxa mortalidade (Brasil e mundo), eficácia das vacinas contra seus efeitos colaterais. 

Há também a pretensão utópica de quem sabe, ajudar a entender melhor o cenário vívido recentemente,
**Perguntas**

1. Qual foi a taxa de mortalidade da COVID-19 por milhão de pessoas no mundo?
2. Esse número flutuou ao ser comparado com os índices brasileiros?
3. Qual a localidade com o maior número de casos reportados?
4. O número de casos teve variação entre os países?  --- RESPONSER
5. Variação entre o Brasil e o mundo sobre a quantidate de pessoas hospitalizadas X mortes. 
6. Tempo entre a primera dose administrada no mundo X Brasil. ok
7. Ritmo de vacinação por país.

#### 2 - Coleta: Coleta (0,5 pt) OK
Os dados utilizados para a análise foram coletados do site https://ourworldindata.org/, que já na pendemia da COVID-19 os disponibilizava de forma pública e gratuita. O site é fruto de um projeto da parceria entre a Universidsde de Oxford, responsável pelas pesquisas  e uma ONG chamada Global Change Data Lab que é responsável pela manutenção do site.
O período utilizado para realizar o MVP foi de março de 2020, período em que foi decretado o "Lock Down" até a dezembro de 2021 quando a maioria das pessoas no mundo já haviam recebido as doses das vacinas e a pandemida estava controlada.
Os arquivos baixados foram os seguintes:
- daily-new-confirmed-covid-19-cases-per-million-people
- daily-new-confirmed-covid-19-deaths-per-million-people
- number-of-covid-19-patients-in-hospital-per-million-people
- daily-covid-19-vaccine-doses-administered-per-million-people

#### 3- Modelagem Modelagem (2,0 pt)

Baseado na características dos dados coletados, o esquema **estrela** escolhido para a modelagem, pelas seguintes razões:


1. **Nível de normanlização apropriado**:

2. **Performance das Queries**:
  
3. **Intuitive Representation**:

4. **Flexibility**:


**Abaixo a imagem de como ficou o modelo:**



![Normalização](./pictures/relacionamento_covid_mvp.jpg)

##### Catálogo de Dados:
1. Tabela location_dimension: Com as seguintes colunas: 
  * location_key(PK) Inteiro: Número inteiro e único na tabela que indica a localidade.
  * entity_name:String
    * Nome da localidade, não é necessariamente um país. Pode ser por exemplo, um continente, ou Mundo menos China, etc.

2. Tabela date_dimension: Com as seguintes colunas:
  * date_key (PK) Inteiro: Número inteiro e único na tabela que indica uma data específica
  * data Date: Traduz o date_key para um formato mais amigável
  * dia Inteiro: Dia do mês
  * mes Inteiro: Mês do ano
  * ano Inteiro: Ano

3. Tabela covid_facts: Com as seguintes colunas:
  * date_key (FK) Inteiro: Para correlação com a tabela date_dimension.
  * location_key (FK) Inteiro: Para correlação com a tabela location_dimension.
  * casos_por_milhao Decimal(10,4): Número de casos reportados por milhão de pessoas.
  * mortes_por_milhao Decimal(10,4): Número de mortes reportadas por milhão de pessoas.
  * hospitalizados_por_milhao Decimal(10,4): Número de pessoas hospitalizadas reportadas por milhão de pessoas.
  * doses_por_milhao Decimal(10,4): Doses administradas de vacina reportados por milhão de pessoas.

#### 4 - Carga. 
##### 4.1 - Extração - Montagem do banco de dados 
##### 4.2 - Carga dos dados
###### Criação das tabelas bronze - Dados originais,sem alteração.
##### 4.3 - Transformação dos dados:

[Clique aqui para acessar o Notebook com os códigos, resultado de queries e gráficos](
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2149961212973897/667217794031682/3413641925918938/latest.html)

#### 5 Análise (3,0 pt). 
##### 5.1 - Respondendo perguntas
###### 5.1.1 - Qual foi a taxa de mortalidade da COVID-19 por milhão de pessoas no mundo?
###### R: Taxa de mortalidade mundial 0.014

![Figura](./pictures/taxa_morte_mundo.jpg)

###### 5.1.2 - Esse número flutuou ao ser comparado com os índices brasileiros?
###### R: Sim - A taxa brasileira foi bem mais que a mundial, 0.028 contra 0.014.

![Figura](./pictures/taxa_morte_brasil.jpg)
###### 5.1.2.1 - Expansão da pergunta - Esse número flutuou ao ser comparado com os índices brasileiros?
###### As taxas brasileiras, estão entram as 15 maiores do mundo? 
###### R: Analisando os 15 países com as maiores taxas de mortalidade, as brasileiras são bem menores, flutuação de 0.01 em comparação com a 15 lugar (North Macedonia). Dois países da América Latina com taxas bem superiores como México com 0.076 e Peru com 0.089.  

![Figura](./pictures/taxa_morte_maiores.jpg)

###### 5.1.2.2- Expansão da pergunta - Esse número flutuou ao ser comparado com os índices brasileiros?
###### R: Analisando os 15 países com as menores taxas de mortalidade, as taxas brasileiras são bem maiores, flutuação muito grande em comparação com a 15 lugar, Bahrain com 0.005.

![Figura](./pictures/taxa_morte_menores.jpg)

###### 5.1.3 - Qual a localidade com o maior número de casos reportados?
###### R: Andorra
![Figura](./pictures/casos_maior.jpg)

###### 5.1.4 O número de casos teve variação entre as localidades? 
###### R: Sim, uma flutuação enorme 279.555 (Andorra) casos contra 9 (Samoa) no entanto, isso se torna irrelevante devido a tamanho da população dos mesmos.

![Figura](./pictures/casos_menor.jpg)

###### 5.1.4.1 Expansão da pergunta - O número de casos teve variação entre as localidades? E com relação ao Brasil?  
###### R: Brasil ficou bem abaixo com 105.909 casos de Andorra. Novamente, isso se torna irrelevante devido a tamanho da população entre os dois países.


![Figura](./pictures/casos_brasil.jpg)

###### 5.1.4.2 Expansão da pergunta - O número de casos teve variação entre as localidades? Tá, mas e a relação do Brasil com localidades semelhantes em termos de população, cultura e com a China que foi a origem da pandemia?
###### R: Espanta a quantidade de casos reportados pela China (país de origem da pandemia) contra as demais localidades. Brasil e Europa quase equivalentes, aumento razoável de caos nos E.U.A.

![Figura](./pictures/casos_comparacao.jpg)

###### 5.1.4.3 Expansão da pergunta - O número de casos teve variação entre as localidades? OK, mas e a taxa de mortalidade?
###### Outro espanto, China com alta taxa com relação aos casos. Brasil esperadamente acima de Estados Unidos e Europa o que índica fragilidade no sistema público de saúde, pois a quantidade de casos é relativamente próxima. Comparando com o México, por ser latino e de dimensões semelhantes às brasileiras ficamos bem atrás em relação à taxa de mortalidade, número que se torna ainda mais expressivo pelo fato do México ter tido apenas 1/3 dos casos reportados por milhão de pessoas.

![Figura](./pictures/taxa_morte_comparacao.jpg)

###### 5.1.6. Tempo entre a primeira dose administrada no mundo X Brasil.
###### R: A diferença é grande entre a primeira dose dada no mundo (oito de dezembro de 2020 na Noruega) e a primeira dose no Brasil (18 de janeiro de 2021), mais de 40 dias. OBS. A data foi extraída a partir do momento em que a contagem de doses passou a ser maior que 0.0001 por milhão de pessoas, logo pode haver uma pequena variação nas datas efetivas. 

![Figura](./pictures/vacina_mundo.jpg)
![Figura](./pictures/vacina_brasil.jpg)

###### 5.1.7. Ritmo de vacinação por país.
###### R: Aqui, foi usado o mesmo parâmetro de escolha das localidades para comparação. Localidades semelhantes em termos de população, cultura e a China que foi a origem da pandemia. Alguns pontos interessantes:
###### 1. Estados Unidos com ritmo muito forte até abril(2021), quando começa a cair até novembro (2021) quando há um leve final aummento e se mantem estável até o período de medição.
###### 2. O ritmo do Brasil, cresce até abril (2021) quando para de crescer. O crescimento volta em junho (2021). Se mantém atá agosto (2021) quando começa a diminuir até o final do período.
###### 3. China tem um crescimento muito grande em maio (2021) que coincide com a queda de vacinação nos E.U.A. Se mantém forte até setembro (2021). Quando começa a cair, em novembro(2021) volta a crescer e tem um pico em dezembro (2021).

![Figura](./pictures/vacina_ritmo.jpg)

###### Aqui, convido para fazerem suas próprias manipulações, clicando em cada uma das localidades, incluindo ou removendo-a do gráfico.
![Figura](./pictures/vacina_contra_morte.jpg)

##### 6. Discussão Geral.
##### A pandemia da COVID-19 causou um impacto muito grande no mundo, consegui tirar como conclusão que o que mais matou não foi a taxa de contaminação em si, mas o nível de preparo dos países com relação aos seus sistemas de saúde, frágil em países em desenvolvimento como o Brasil. 
##### A taxa de contaminação variou bastante entre as localidades, sendo alta até mesmo em localidades mais desenvolvidas como, E.U.A e Europa. Por outro lado, foi surpreendentemente baixa na China que foi a origem do vírus.
##### As vacinas foram super importantes para o fim da pandemia, mas houve um determinado período onde os países em desenvolvimento não conseguiram manter um ritmo adequado, muito provavelmente devido à alta demanda das localidades que de fato produziram suas vacinas, como a Europa com a AztraZeneca, Pfizer e Janssen nos E.U.A, e Corona-Vac na China.
