from Tabela import Tabela

tab = Tabela()

#====== Criar tabela "teste" ======
#tab.criar_tabela("teste",{"id": ["integer",6],"nome":["string",20],"idade":["integer",3],"telefone":["integer",9],"estado":["string",2],"dre":["integer",9]}, tipo="heap_variavel", hash_por="idade")

#====== Usar tabela "teste" ======
tab.abrir_tabela("teste")

#====== Pegar conteúdo de condetudo.csv ======
#f = open("gen_data/conteudo.csv", "r")
#conteudo = ''
#for linha in f:
#    conteudo += linha
#f.close()  

#====== Inserir conteúdo (operação um pouco demorada para grande volume de dados, principalmente no caso ordenado) ======
#tab.inserir(conteudo)

#====== Selecionar registros ======
print(tab.selecionar_registros({"atributo": "dre", "valor": "677285586", "valor2": "0" }, sinal="between", first=False))

#====== Deletar registros ======
#tab.delete_registros({"atributo": "dre", "valor": "677285586", "valor2": "88" }, sinal="between", first=False)
#tab.delete_registros({"atributo": "idade", "valor": "88", "valor2": "8" }, sinal="between", first=False)