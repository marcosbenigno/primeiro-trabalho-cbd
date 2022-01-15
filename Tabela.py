import json
import math
from datetime import datetime
import os
import shutil
import string

class Tabela:
    def criar_tabela(self, nome, esquema, tipo="heap_fixo", ordenado_por=None, hash_por=None):
        self.nome = nome
        self.bd_em_uso = nome
        self.tipo = tipo
        self.ordenado_por = ordenado_por
        self.hash_por = hash_por
        self.tamanho_bloco = 4000
        if os.path.exists(nome):
            shutil.rmtree(nome)
        os.makedirs(nome)
        self.criar_head(esquema, tipo)
        
    def abrir_tabela(self, nome):
        self.nome = nome
        head = self.pegar_head()
        self.tipo = head["tipo_arquivo"]
        
    def formatar_campo(self, valor, tamanho_do_campo):
        conteudo_a_salvar = ''
        if (len(valor) <  tamanho_do_campo):
            if (self.tipo == "heap_fixo" or self.tipo == "ordenado" or self.tipo == "hash"):
                pan = (tamanho_do_campo - len(valor)) * ' '
                conteudo_a_salvar += str(pan+valor)
            elif (self.tipo == "heap_variavel"):
                conteudo_a_salvar += valor
        elif (len(valor) >  tamanho_do_campo):
            if (self.tipo == "heap_fixo" or self.tipo == "heap_variavel" or self.tipo == "ordenado" or self.tipo == "hash"):
                
                conteudo_a_salvar += str(valor)[: tamanho_do_campo ]
        else: 
            if (self.tipo == "heap_fixo" or self.tipo == "heap_variavel" or self.tipo == "ordenado" or self.tipo == "hash"):
                conteudo_a_salvar += str(valor) 
        return conteudo_a_salvar
        
        
    def inserir(self, csv, reorganizar=False):
        #csv pode ser caminho pra csv ou lista de strings csv
        esquema = self.pegar_head()["esquema"]
        atributos = list(esquema.keys())
        conteudo_a_salvar = ''
        if (isinstance(csv, str)):
            csv = csv.split("\n")
        
        head = self.pegar_head()
             
        if (isinstance(csv, list)):
            for registro in csv:
                head["n_registros"] += 1
                conteudo_a_salvar = ' '
                if (self.tipo == "hash"): 
                    valores = registro.split(',')
                    for index, valor in enumerate(valores):
                        tamanho_do_campo = esquema[atributos[index]][1]
                        conteudo_a_salvar += self.formatar_campo(valor, tamanho_do_campo)
                        conteudo_a_salvar += ";"
                    conteudo_a_salvar += "\n"
                    atbrs =list(head["esquema"].keys())
                    valor_a_hashear = valores[atbrs.index(head["hash_por"])]
                    chave_hash = self.hash(valor_a_hashear, 100)
                    
                    #caso 1
                    temDeletados = False
                    deletadoItem = ''
                    for index, item in enumerate(head['deletados']):
                        if (str(chave_hash) == item[0] or (len(item[0].split("_")) == 3 and str(chave_hash) == item[0].split("_")[1]) ):
                            temDeletados = True
                            deletadoItem = item
                            del head['deletados'][index]
                            break
                        
                    if (temDeletados):
                        bloco = deletadoItem[0]
                        linha = deletadoItem[1]
                        i = 0
                        novo_bloco = ''
                        f = open(self.nome+"/"+str(bloco), "r+")
                        for registro in f:
                            if (i == linha):
                                novo_bloco += conteudo_a_salvar
                            else:
                                novo_bloco += registro
                            i = i + 1
                        f.seek(0)
                        f.truncate()
                        f.write(novo_bloco)
                        f.close()  
                    #caso 2
                    elif (not(os.path.isfile(self.nome+"/"+str(chave_hash))) or (os.path.isfile(self.nome+"/"+str(chave_hash)) and os.path.getsize(self.nome+"/"+str(chave_hash)) <= head["bfr"] * head["tamanho_registro"] + head["tamanho_registro"])):
                        f = open(self.nome+"/"+str(chave_hash), "a")
                        f.write(conteudo_a_salvar)
                        head["insercoes_sem_comprimir"] += 1
                        f.close()
                
                        self.comprimir(head["insercoes_sem_comprimir"])
                    
                    else:
                        #caso 3
                        item = ''
                        for index, val in enumerate(head["deletados"]):
                            if (val[0].split('_')[0] == chave_hash):
                                item = val[1]
                                del head["deletados"][index]
                                break
                        if (item != ''):
                            #caso 3
                            bloco = item[0]
                            linha = item[1]
                            i = 0
                            novo_bloco = ''
                            f = open(self.nome+"/"+str(bloco), "r+")
                            for registro in f:
                                if (i == linha):
                                    novo_bloco += conteudo_a_salvar
                                else:
                                    novo_bloco += registro
                                i = i + 1
                            f.seek(0)
                            f.truncate()
                            f.write(novo_bloco)
                            f.close()  
                        else:
                            #caso 4
                            arquivos = [f for f in os.listdir(self.nome+"/") if (os.path.isfile(os.path.join(self.nome+"/", f)) and (len(f) == 3 and f.split('_')[1] == chave_hash))]
                            maior = 0
                            for item in arquivos:
                                if (item.split('_')[2] > maior):
                                    maior = item.split('_')[2]
                            bloco = maior
                            if (not(not(os.path.isfile(self.nome+"/overflow_"+str(chave_hash)+"_"+str(bloco))) or (os.path.isfile(self.nome+"/overflow_"+str(chave_hash)+"_"+str(bloco)) and os.path.getsize(self.nome+"/overflow_"+str(chave_hash)+"_"+str(bloco)) + head["tamanho_registro"] <= head["bfr"] * head["tamanho_registro"] * 2 ))):
                                bloco = bloco + 1
                            
                            f = open(self.nome+"/overflow_"+str(chave_hash)+"_"+str(bloco), "a")
                            f.write(conteudo_a_salvar)
                            head["insercoes_sem_comprimir"] += 1
                            f.close()
                           
                            self.comprimir(head["insercoes_sem_comprimir"])                  
                else:
                    if (self.tipo == "heap_fixo" or self.tipo == "heap_variavel" or self.tipo == "ordenado"):
                        #formatar registro
                        valores = registro.split(',')
                        for index, valor in enumerate(valores):
                            tamanho_do_campo = esquema[atributos[index]][1]
                            conteudo_a_salvar += self.formatar_campo(valor, tamanho_do_campo)
                            conteudo_a_salvar += ";"
                        conteudo_a_salvar += "\n"
                    
                    if (len(head["deletados"]) != 0 and self.tipo != "ordenado"):
       
                        item = head["deletados"].pop()
                        
                        bloco = item[0]
                        linha = item[1]
                        i = 0
                        novo_bloco = ''

                        f = open(self.nome+"/"+str(bloco), "r+")

                        for registro in f:
                            if (i == linha):
                                novo_bloco += conteudo_a_salvar
                            else:
                                novo_bloco += registro
                            i = i + 1
                        f.seek(0)
                        f.truncate()
                        f.write(novo_bloco)
                        f.close()  
                    else:
                        if (self.tipo == "heap_fixo"):
                            if (head["registros_disponiveis_bloco_atual"] > 0):
                                head["registros_disponiveis_bloco_atual"] = head["registros_disponiveis_bloco_atual"] - 1
                            else:
                                head["bloco_a_adicionar"] = head["bloco_a_adicionar"] + 1
                                head["registros_disponiveis_bloco_atual"] = head["bfr"]
                    
                            f = open(self.nome+"/"+str(head["bloco_a_adicionar"]), "a")
                            f.write(conteudo_a_salvar)
                            head["insercoes_sem_comprimir"] += 1
                            f.close()
                            
                            self.comprimir(head["insercoes_sem_comprimir"])
                        elif (self.tipo == "heap_variavel"):
                            if (os.path.isfile(self.nome+"/"+str(head["bloco_a_adicionar"]))):
                                if (os.path.getsize(self.nome+"/"+str(head["bloco_a_adicionar"])) + len(conteudo_a_salvar) <= head["tamanho_bloco"]):
                                    head["bloco_a_adicionar"] = head["bloco_a_adicionar"]
                                else:
                                    head["bloco_a_adicionar"] = head["bloco_a_adicionar"] + 1
                            f = open(self.nome+"/"+str(head["bloco_a_adicionar"]), "a")
                            f.write(conteudo_a_salvar)
                            head["insercoes_sem_comprimir"] += 1
                            f.close()
                            
                            self.comprimir(head["insercoes_sem_comprimir"])
                            
                        elif (self.tipo == "ordenado"):
                            if (reorganizar):
                                if (head["registros_disponiveis_bloco_atual"] > 0):
                                    head["registros_disponiveis_bloco_atual"] = head["registros_disponiveis_bloco_atual"] - 1
                                else:
                                    head["bloco_a_adicionar"] = head["bloco_a_adicionar"] + 1
                                    head["registros_disponiveis_bloco_atual"] = head["bfr"]
                        
                                f = open(self.nome+"/"+str(head["bloco_a_adicionar"]), "a")
                                f.write(conteudo_a_salvar)
                                head["insercoes_sem_comprimir"] += 1
                                f.close()
                               
                                self.comprimir(head["insercoes_sem_comprimir"])
                            else:
                                f = open(self.nome+"/overflow", "a")
                                f.write(conteudo_a_salvar)
                                head["insercoes_sem_comprimir"] += 1
                                f.close()
                            
        if (self.tipo == "ordenado" and  not reorganizar and os.path.getsize(self.nome+"/overflow") > head["tamanho_bloco"]*2):                 
            self.reorganizar(head["insercoes_sem_comprimir"])
            return
        self.atualizar_head({"registros_disponiveis_bloco_atual": head["registros_disponiveis_bloco_atual"],
                        "bloco_a_adicionar": head["bloco_a_adicionar"],
                        "deletados": head["deletados"],
                        "n_registros": head["n_registros"]})
                
        
        
    def reorganizar(self, insercoes_sem_comprimir):
        head = self.pegar_head()
        blocosCsv = self.mergeBlocos()
        blocosJson = self.toJSON(blocosCsv)
        tipo = head["esquema"][head["ordenado_por"]][0]
      
        organizado = {}
        if (tipo == "float" or tipo == "integer"):
            organizado = sorted(blocosJson, key=lambda x: float((x[head["ordenado_por"]]).strip()))
        elif (tipo == "string"):
            organizado = sorted(blocosJson, key=lambda x: (x[head["ordenado_por"]]).strip()) 
        self.removerBlocos()
        self.inserir(self.toCSV(organizado).split("\n")[:-1], reorganizar=True)
        f = open(self.nome+"/overflow", "r+")
        f.seek(0)
        f.truncate()
        f.write('')
        f.close()
        self.comprimir(insercoes_sem_comprimir)
      
    def removerBlocos(self):
        head = self.pegar_head()
        for bloco in range(head["bloco_a_adicionar"] + 1):
            if (os.path.isfile(self.nome+"/"+str(bloco))):
                os.remove(self.nome+"/"+str(bloco))
        head["bloco_a_adicionar"] = 0
        self.atualizar_head({"bloco_a_adicionar": 0})
        

        
    def mergeBlocos(self):
        merged = ''
        head = self.pegar_head()
        for bloco in range(head["bloco_a_adicionar"] + 1):
            if (os.path.isfile(self.nome+"/"+str(bloco))):
                f = open(self.nome+"/"+str(bloco), "r+")
                for registro in f:
                    merged += registro
                    
        if (os.path.isfile(self.nome+"/overflow")):
            f = open(self.nome+"/overflow", "r+")
            for registro in f:
                merged += registro
        return merged
        
    def toJSON(self, csv):
        head = self.pegar_head()
        registros = csv.split("\n")
        posicao = list(head["esquema"].keys())
        resultado = []
        for registro in registros:
            if (len(registro.strip()) > 1 and registro[0] != "#"):
                campos = registro[1:].split(";")[:-1]
                
                indice = 0
                reg = {}
                for campo in campos:
                    reg[posicao[indice]] = campo
                    indice += 1
                resultado.append(reg)
        return resultado
            
    def toCSV(self, inputLista):
        csv = ''
        
        for registro in inputLista:
            for atributo in registro:
                csv += registro[atributo] + ","
            csv = csv[:-1]
            csv += "\n"
           
        return csv
            
    def pegar_head(self):
        f = open(self.nome+"/head", "r")
        head = json.loads(f.readline())
        f.close()
        return head
        
    def atualizar_head(self, atualizacoes):
        head = self.pegar_head()
        for atributo in atualizacoes:
            head[atributo] = atualizacoes[atributo]
        now = datetime.utcnow().strftime("%m/%d/%Y, %H:%M:%S")
        head["alterado_em"] = now
        f = open(self.nome+"/head", "w")
        f.write(json.dumps(head))
        f.close()
        return head

        
        
    def criar_head(self, esquema, tipo):
        now = datetime.utcnow().strftime("%m/%d/%Y, %H:%M:%S")
        head = {}
        head["esquema"] = esquema
        head["n_registros"] = 0
        head["bfr"] = self.bfr(esquema) - 1
        head["tamanho_registro"] = self.tamanho_registro(esquema)
        head["deletados"] = []
        head["criado_em"] = now
        head["alterado_em"] = now
        head["bloco_a_adicionar"] = 0
        head["localizacao_relativa"] = self.localizacao_relativa(esquema)
        head["registros_disponiveis_bloco_atual"] = self.bfr(esquema)
        head["tipo_arquivo"] = tipo
        head["insercoes_sem_comprimir"] = 0
        head["tamanho_bloco"] = self.tamanho_bloco
        head["ordenado_por"] = self.ordenado_por
        head["hash_por"] = self.hash_por
        
        f = open(self.nome+"/head", "w")
        f.write(json.dumps(head))
        f.close()
        return head
    
    def localizacao_relativa(self, esquema):
        tamanho = 0
        localizacoes = {}
        i = 0
        anterior = 0
        for atributo in esquema:
            
            if (i == 0):
                localizacoes[atributo] = 1  
                tamanho = esquema[atributo][1]
                anterior = 1
                i = i + 1
            else:
                localizacoes[atributo] = anterior  + tamanho + 1
                anterior = anterior  + tamanho + 1
                tamanho = esquema[atributo][1]
                
                i = i + 1

        return localizacoes


    def bfr(self, esquema):
        return math.floor(self.tamanho_bloco/self.tamanho_registro(esquema))
        

    def tamanho_registro(self, esquema):
        tamanho = 0
        for atributo in esquema:
                
                tamanho += esquema[atributo][1] + 2 + 1 + 1
        return tamanho
        
    def bloco_esta_no_intervalo(self, bloco, localizacao_relativa, tamanho_campo, tipo, campo1, campo2, sinal):
        head = self.pegar_head()
        f = open(self.nome + "/" + bloco, "r")
        f.seek(0)
        registro1 = f.readline()[1:]
   
        atributo1 = registro1[localizacao_relativa - 1 : localizacao_relativa + tamanho_campo - 1]
      
        

        if (tipo == "string"):
            campo1 = str(campo1)
            atributo1 = str(atributo1)
            if (campo2 != None):
                campo2 = str(campo2)
            
        elif (tipo == "float" or tipo == "integer"):
            atributo1 = float(atributo1)
            campo1 = float(campo1)
            if (campo2 != None):
                campo2 = float(campo2)
            
            
        f.close()
        
        if (sinal == "between"):
            return atributo1 <= max(campo1, campo2) and atributo1 >= min(campo1, campo2)
        elif (sinal == "<"):
            return atributo1 <= campo1
        elif (sinal == "<="):
            return atributo1 <= campo1
        elif (sinal == ">"):
            return atributo1 >= campo1
        elif (sinal == ">="):
            return atributo1 >= campo1
        elif (sinal == "!="):
            return True
        elif (sinal == "="):
            return atributo1 <= campo1
        
        
        
        
    def delete_registros(self, atributo, first=False, sinal="="):
    
        #iterar atributos e ir marcando
        head = self.pegar_head()
        localizacao_relativa = head["localizacao_relativa"][atributo["atributo"]]
        tamanho_campo = head["esquema"][atributo["atributo"]][1]
        dont_look_others = False
        posicao = list(head["esquema"].keys()).index(atributo["atributo"])
        blocos_acessados = 0

        if (sinal != "between"):
            atributo["valor2"] = None

        if (self.tipo == "hash" and head["hash_por"] == atributo["atributo"]):
            if (sinal == "="):
                chave_hash = self.hash(atributo["valor"], 100)
                novo_bloco = ''
                escrever = False
                deletado = 0
                f = open(self.nome+"/"+str(chave_hash), "r+")
                blocos_acessados += 1
                linha = 0
           
                #hash
                for registro in f:
                    if (registro[0] != "#"):
                        valor_registro_atual = registro[localizacao_relativa : localizacao_relativa + tamanho_campo].strip()
                        if (self.expressao(valor_registro_atual, atributo["valor"], sinal, atributo["valor2"], head["esquema"][atributo["atributo"]][0]) and not(dont_look_others)):
                            nova_linha = "#" + registro[1:]
                            novo_bloco += nova_linha
                            deletado += 1
                            head["deletados"] += [self.nome+"/"+str(chave_hash), linha]
                            escrever = True
                            if (first):
                                dont_look_others = True
                        else:
                            novo_bloco += registro
                    else:
                        novo_bloco += registro
                    linha += 1
                if (escrever):
                    f.seek(0)
                    f.truncate()
                    head["n_registros"] -= deletado
                    
                    self.atualizar_head({"n_registros": head["n_registros"], "deletados": head["deletados"]})
                    f.write(novo_bloco)
                    escrever = False
                    if (first):
                        f.close()
                        print(str(blocos_acessados) + " blocos acessados")
                        return
                f.close()         
                #overflow  do hash  
                if (not dont_look_others):
                    deletado
                    arquivos = []
                    for arq in os.listdir(self.nome+"/"):
                        if (os.path.isfile(self.nome+"/" + arq) and (len(arq.split("_")) == 3 and arq.split('_')[1] == str(chave_hash))):
                            arquivos.append(arq)
                    
                    for arquivo in arquivos:
                        novo_bloco = ''
                        f = open(self.nome+"/"+arquivo, "r+")
                        blocos_acessados += 1
                        linha = 0
                        for registro in f:
                            if (registro[0] != "#"):
                                valor_registro_atual = registro[localizacao_relativa : localizacao_relativa + tamanho_campo].strip()
                                if (self.expressao(valor_registro_atual, atributo["valor"], sinal, atributo["valor2"], head["esquema"][atributo["atributo"]][0]) and not(dont_look_others)):
                                    nova_linha = "#" + registro[1:]
                                    novo_bloco += nova_linha
                                    head["deletados"] += [self.nome+"/"+str(chave_hash), linha]
                                    deletado += 1
                                    escrever = True
                                    if (first):
                                        dont_look_others = True
                                else:
                                    novo_bloco += registro
                            else:
                                novo_bloco += registro
                            linha += 1    
                        if (escrever):
                            f.seek(0)
                            f.truncate()
                            head["n_registros"] -= deletado
                            self.atualizar_head({"n_registros": head["n_registros"], "deletados": head["deletados"]})
                            f.write(novo_bloco)
                            escrever = False
                            if (first):
                                f.close()
                                print(str(blocos_acessados) + " blocos acessados")
                                return
                        f.close()     
          
                dont_look_others = False
                
                
        if ((self.tipo == "hash" and sinal != "=") or (self.tipo == "hash" and head["hash_por"] != atributo["atributo"])):
            # hash com sinal diferente de "="
            escrever = False
            arquivos = [f for f in os.listdir(self.nome+"/") if (os.path.isfile(os.path.join(self.nome+"/", f)) and (len(f) > 0 and f != "head"))]
            
            for arquivo in arquivos:
                f = open(self.nome+"/"+arquivo, "r+")
                blocos_acessados += 1
                novo_bloco = ''
                linha = 0
                deletado = 0
                for registro in f:
                    if (registro[0] != "#"):
                        valor_registro_atual = registro[localizacao_relativa : localizacao_relativa + tamanho_campo].strip()
                        
                        if (self.expressao(valor_registro_atual, atributo["valor"], sinal, atributo["valor2"], head["esquema"][atributo["atributo"]][0]) and not(dont_look_others)):
                            nova_linha = "#" + registro[1:]
                            novo_bloco += nova_linha
                            head["deletados"].append([arquivo, linha])
                            deletado += 1    
                            escrever = True
                            if (first):
                                dont_look_others = True
                        else:
                            novo_bloco += registro
                    else:
                        novo_bloco += registro
                    linha += 1    
                if (escrever):
                    f.seek(0)
                    f.truncate()
                    head["n_registros"] -= deletado
                    self.atualizar_head({"n_registros": head["n_registros"], "deletados": head["deletados"]})
                    f.write(novo_bloco)
                    escrever = False
                    if (first):
                        f.close()
                        print(str(blocos_acessados) + " blocos acessados")
                        return
                f.close()     
     
            dont_look_others = False
        if (self.tipo == "ordenado"):
            #overflow
            if (os.path.isfile(self.nome+"/overflow")):
                novo_bloco = ''
                escrever = False
                f = open(self.nome+"/overflow", "r+")
                blocos_acessados += 1
                linha = 0
                deletado = 0
                for registro in f:    
                    if (registro[0] != "#"):
                        valor_registro_atual = registro[localizacao_relativa : localizacao_relativa + tamanho_campo].strip()
                        if (self.expressao(valor_registro_atual, atributo["valor"], sinal, atributo["valor2"], head["esquema"][atributo["atributo"]][0]) and not(dont_look_others)):
                            nova_linha = "#" + registro[1:]
                            novo_bloco += nova_linha
                            deletado += 1
                            escrever = True
                            if (first):
                                dont_look_others = True
                        else:
                            novo_bloco += registro
                    else:
                        novo_bloco += registro
                    linha += 1
                if (escrever):
                    f.seek(0)
                    f.truncate()
                    head["n_registros"] -= deletado
                    self.atualizar_head({"n_registros": head["n_registros"]})
                    f.write(novo_bloco)
                    escrever = False
                    if (first):
                        f.close()
                        print(str(blocos_acessados) + " blocos acessados")
                        return
                f.close()     
           
        dont_look_others = False
        if (self.tipo == "heap_fixo" or self.tipo == "heap_variavel" or (self.tipo == "ordenado" and head["ordenado_por"] != atributo["atributo"])):
            for bloco in range(head["bloco_a_adicionar"] + 1):
                novo_bloco = ''
                escrever = False
                if (os.path.isfile(self.nome+"/"+str(bloco))):
                    
                    f = open(self.nome+"/"+str(bloco), "r+")
                    blocos_acessados += 1
                    linha = 0
                    deletado = 0
                    for registro in f:
                        
                        if (registro[0] != "#"):
                            valor_registro_atual = '' 
                            if (self.tipo == "heap_fixo" or self.tipo == "ordenado"):
                                valor_registro_atual = registro[localizacao_relativa : localizacao_relativa + tamanho_campo].strip()
                            elif (self.tipo == "heap_variavel"):
                                valor_registro_atual = registro.split(";")[posicao]
                            if (self.expressao(valor_registro_atual, atributo["valor"], sinal, atributo["valor2"], head["esquema"][atributo["atributo"]][0]) and not(dont_look_others)):
                                nova_linha = "#" + registro[1:]
                                novo_bloco += nova_linha
                                head["deletados"].append([bloco, linha])
                                deletado += 1
                                escrever = True
                                if (first):
                                    dont_look_others = True
                            else:
                                novo_bloco += registro
                        else:
                            novo_bloco += registro
                        linha += 1
                    if (escrever):
                        f.seek(0)
                        f.truncate()
                        head["n_registros"] -= deletado
                        self.atualizar_head({"deletados": head["deletados"], "n_registros": head["n_registros"]})
                        f.write(novo_bloco)
                        
                        escrever = False
                        if (first):
                            f.close()
                            print(str(blocos_acessados) + " blocos acessados")
                            return
                    f.close()
        elif (self.tipo != "hash"):
            for bloco in range(head["bloco_a_adicionar"] + 1):
                novo_bloco = ''
                escrever = False
                deletado = 0
                if (os.path.isfile(self.nome+"/"+str(bloco))):
                    f = ''
                    if (self.bloco_esta_no_intervalo(str(bloco), localizacao_relativa, tamanho_campo, head["esquema"][atributo["atributo"]][0], atributo["valor"], atributo["valor2"], sinal)):
                        f = open(self.nome+"/"+str(bloco), "r+")
                        blocos_acessados += 1
                        linha = 0
                    else:
                        continue
                    for registro in f:
                        
                        if (registro[0] != "#"):
                            valor_registro_atual = registro[localizacao_relativa : localizacao_relativa + tamanho_campo].strip()
            
                            if (self.expressao(valor_registro_atual, atributo["valor"], sinal, atributo["valor2"], head["esquema"][atributo["atributo"]][0]) and not(dont_look_others)):
                                nova_linha = "#" + registro[1:]
                                novo_bloco += nova_linha
                                head["deletados"].append([bloco, linha])
                                deletado += 1
                                escrever = True
                                if (first):
                                    dont_look_others = True
                            else:
                                novo_bloco += registro
                        else:
                            novo_bloco += registro
                        linha += 1
                    if (escrever):
                        f.seek(0)
                        f.truncate()
                        head["n_registros"] -= deletado
                        self.atualizar_head({"deletados": head["deletados"], "n_registros": head["n_registros"]})
                        f.write(novo_bloco)
                        
                        escrever = False
                        if (first):
                            f.close()
                            print(str(blocos_acessados) + " blocos acessados")
                            return
                    f.close()
        print(str(blocos_acessados) + " blocos acessados")


    def selecionar_registros(self, atributo, first=False, sinal="="):
        head = self.pegar_head()
        localizacao_relativa = head["localizacao_relativa"][atributo["atributo"]]
        tamanho_campo = head["esquema"][atributo["atributo"]][1]
        posicao = list(head["esquema"].keys()).index(atributo["atributo"])
        blocos_acessados = 0
        if (sinal != "between"):
            atributo["valor2"] = None
        resultado = ''
        
        if (self.tipo == "hash" and head["hash_por"] == atributo["atributo"]):
            if (sinal == "="):
                chave_hash = self.hash(atributo["valor"], 100)
                novo_bloco = ''

                f = open(self.nome+"/"+str(chave_hash), "r+")
                linha = 0
                blocos_acessados += 1
                #hash
                for registro in f:
                    if (registro[0] != "#"):
                        valor_registro_atual = registro[localizacao_relativa : localizacao_relativa + tamanho_campo].strip()
                        if (self.expressao(valor_registro_atual, atributo["valor"], sinal, atributo["valor2"], head["esquema"][atributo["atributo"]][0])):
                            novo_bloco += registro
                            if (first):
                                print(str(blocos_acessados)+ " blocos acessados")
                                return novo_bloco

                #overflow    
                arquivos = []
                for arq in os.listdir(self.nome+"/"):
                    if (os.path.isfile(self.nome+"/" + arq) and (len(arq.split("_")) == 3 and arq.split('_')[1] == str(chave_hash))):
                        arquivos.append(arq)

                for arquivo in arquivos:
                    f = open(self.nome+"/"+arquivo, "r+")
                    blocos_acessados += 1
                    for registro in f:
                        if (registro[0] != "#"):
                            valor_registro_atual = registro[localizacao_relativa : localizacao_relativa + tamanho_campo].strip()
                            if (self.expressao(valor_registro_atual, atributo["valor"], sinal, atributo["valor2"], head["esquema"][atributo["atributo"]][0])):
                            
                                novo_bloco += registro

                                if (first):
                                    print(str(blocos_acessados)+ " blocos acessados")
                                    return novo_bloco
                print(str(blocos_acessados)+ " blocos acessados")
                return novo_bloco
                
        if ((self.tipo == "hash" and sinal != "=") or (self.tipo == "hash" and head["hash_por"] != atributo["atributo"])):
            novo_bloco = ''
           
            arquivos = [f for f in os.listdir(self.nome+"/") if (os.path.isfile(os.path.join(self.nome+"/", f)) and (len(f) > 0 and f != "head"))]
            
            for arquivo in arquivos:
                f = open(self.nome+"/"+arquivo, "r+")
                blocos_acessados += 1
                for registro in f:
                    if (registro[0] != "#"):
                        valor_registro_atual = registro[localizacao_relativa : localizacao_relativa + tamanho_campo].strip()
                        
                        if (self.expressao(valor_registro_atual, atributo["valor"], sinal, atributo["valor2"], head["esquema"][atributo["atributo"]][0])):
                           
                            novo_bloco += registro
                           
                            if (first):
                                print(str(blocos_acessados) + " blocos acessados")
                                return novo_bloco
            print(str(blocos_acessados) + " blocos acessados")
            return novo_bloco
       
       
        if (self.tipo == "ordenado"):
            #overflow
            if (os.path.isfile(self.nome+"/overflow")):
                f = open(self.nome+"/overflow", "r+")
                blocos_acessados += 1
                for registro in f:    
                    if (registro[0] != "#"):
                        valor_registro_atual = registro[localizacao_relativa : localizacao_relativa + tamanho_campo].strip()
                        if (self.expressao(valor_registro_atual, atributo["valor"], sinal, atributo["valor2"], head["esquema"][atributo["atributo"]][0])):
                            resultado += registro
                
                            if (first):
                                f.close()
                                print(str(blocos_acessados) + " blocos acessados")
                                return resultado
                f.close()
          
            
        if (self.tipo == "heap_fixo" or self.tipo == "heap_variavel" or (self.tipo == "ordenado" and head["ordenado_por"] != atributo["atributo"])):
            for bloco in range(head["bloco_a_adicionar"] + 1):       
                if (os.path.isfile(self.nome+"/"+str(bloco))):

                    f = open(self.nome+"/"+str(bloco), "r+")
                    blocos_acessados += 1
                    for registro in f:
                        valor_registro_atual = ''
                        if (registro[0] != "#"):
                            
                            if (self.tipo == "heap_fixo" or self.tipo == "ordenado"):
                                valor_registro_atual = registro[localizacao_relativa : localizacao_relativa + tamanho_campo].strip()
                                
                            elif (self.tipo == "heap_variavel"):
                                valor_registro_atual = registro.split(";")[posicao]
                            
                            if (self.expressao(valor_registro_atual, atributo["valor"], sinal, atributo["valor2"], head["esquema"][atributo["atributo"]][0])):
                                resultado += registro
                                
                                if (first):
                                    f.close()
                                    print(str(blocos_acessados) + " blocos acessados")
                                    return resultado
                    f.close()
            print(str(blocos_acessados) + " blocos acessados")
            return resultado
        else:
            #ordenado
            for bloco in range(head["bloco_a_adicionar"] + 1):
                if (os.path.isfile(self.nome+"/"+str(bloco))):
                    f = ''
                   
                    if (self.bloco_esta_no_intervalo(str(bloco), localizacao_relativa, tamanho_campo, head["esquema"][atributo["atributo"]][0], atributo["valor"], atributo["valor2"], sinal)):
                        f = open(self.nome+"/"+str(bloco), "r+")
                        blocos_acessados += 1
                        
                    else:
                        continue
                    for registro in f:
                        
                        if (registro[0] != "#"):
                            valor_registro_atual = registro[localizacao_relativa : localizacao_relativa + tamanho_campo].strip()
            
                            if (self.expressao(valor_registro_atual, atributo["valor"], sinal, atributo["valor2"], head["esquema"][atributo["atributo"]][0])):
                                
                                resultado += registro
                                
                               
                                if (first):
                                    f.close()
                                    print(str(blocos_acessados) + " blocos acessados")
                                    return resultado
                    f.close()
            print(str(blocos_acessados) + " blocos acessados")
            return resultado            
        
    def comprimir(self, insercoes_sem_comprimir):
        head = self.pegar_head()
        if (self.tipo == "ordenado"):
            novo_bloco = ''
            f = open(self.nome+"/overflow", "r+")
            for registro in f:
                if (registro[0] != "#"):
                    novo_bloco += registro
            f.seek(0)
            f.truncate()
            f.write(novo_bloco)
            f.close()
            self.atualizar_head({"insercoes_sem_comprimir": 0}) 
        if (self.tipo == "heap_fixo" or self.tipo == "heap_variavel" or self.tipo == "ordenado"):
            if (len(head["deletados"]) * head["tamanho_registro"] >= (0.2 * (head["n_registros"] * head["tamanho_registro"])) and len(head["deletados"]) > 0):
                for bloco in range(head["bloco_a_adicionar"] + 1):
                    novo_bloco = ''
                    f = open(self.nome+"/"+str(bloco), "r+")
                    for registro in f:
                        if (registro[0] != "#"):
                            novo_bloco += registro
                    f.seek(0)
                    f.truncate()
                    f.write(novo_bloco)
                    f.close()
                    self.atualizar_head({"insercoes_sem_comprimir": 0, "deletados": []})
  
            else:
                self.atualizar_head({"insercoes_sem_comprimir": insercoes_sem_comprimir})
        if (self.tipo == "hash"):
             if (len(head["deletados"]) * head["tamanho_registro"] >= (0.2 * (head["n_registros"] * head["tamanho_registro"])) and len(head["deletados"]) > 0):
                arquivos = [f for f in os.listdir(self.nome+"/") if (os.path.isfile(os.path.join(self.nome+"/", f)) and (len(f) > 0 and f != "head"))]
                for bloco in arquivos:
                    novo_bloco = ''
                    f = open(self.nome+"/"+str(bloco), "r+")
                    for registro in f:
                        if (registro[0] != "#"):
                            novo_bloco += registro
                    f.seek(0)
                    f.truncate()
                    f.write(novo_bloco)
                    f.close()
                    self.atualizar_head({"insercoes_sem_comprimir": 0, "deletados": []})
                    
    def hash(self, texto, maxBlocos):
        head = self.pegar_head()
        esquema = head["esquema"]
        hash_por = head["hash_por"]
        caracteres = list(string.printable)
        intervaloTexto = str(texto)[:esquema[hash_por][1]][:6]
        numero = ''
        for caracter in intervaloTexto:
            numero += str(caracteres.index(caracter))
        return int(numero) % maxBlocos

    def expressao(self, valor_registro_atual, range1, sinal, range2, tipo):
        if (tipo == "string"):
            valor_registro_atual = str(valor_registro_atual)
            range1 = str(range1)
            if (range2 != None):
                range2 = str(range2)
            
        elif (tipo == "float" or tipo == "integer"):
            valor_registro_atual = float(valor_registro_atual)

            range1 = float(range1)
            if (range2 != None):
                range2 = float(range2)      
                
        if (sinal == "between"):
            return valor_registro_atual >= min(range1, range2) and valor_registro_atual <= max(range1, range2)
        elif (sinal == "<"):
            return valor_registro_atual < range1
        elif (sinal == "<="):
            return valor_registro_atual <= range1
        elif (sinal == ">"):
            return valor_registro_atual > range1
        elif (sinal == ">="):
            return valor_registro_atual >= range1
        elif (sinal == "!="):
            return valor_registro_atual != range1
        elif (sinal == "="):
            return valor_registro_atual == range1
        

