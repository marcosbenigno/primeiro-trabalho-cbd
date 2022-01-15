
import names
import random

states = ['AC','AL','AM','AP','BA','CE','DF','ES','GO','MA','MG','MS','MT','PA','PB','PE','PI','PR','RJ','RN','RO','RR','RS','SC','SE','SP','TO']
conteudo = ''
for index in range(20000):
    conteudo += str(index) + ','
    conteudo += str(names.get_full_name()) + ','
    conteudo += str(random.randint(0, 150)) + ','
    conteudo += str(random.randrange(0, 999999999, 9)) + ','
    conteudo += str(states[random.randint(0,len(states)-1)]) + ','
    conteudo += str(random.randrange(0, 999999999, 9)) + '\n'

f = open("conteudo.csv", "a")
                        
f.write(conteudo)
f.close()  