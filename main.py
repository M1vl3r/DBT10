from models.cofINFO import confINFO
from models.human import human

human = human()
confINFO = confINFO()

for row, list in enumerate(human.selectHumanCost()):
    print(row, ')', list)


#for row, list in enumerate(human.cost_pay()):
   # print(row, ')', list)

#for row, list in enumerate(human.tezisdCity()):
    #print(row, ')', list)

for row, list in enumerate(confINFO.neededHostelonCity()):
    print(row, ')', list)


