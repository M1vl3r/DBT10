from models.model import Model
class human(Model):
    __nameTable = 'human'
    __first = 'firts_name'
    __second = 'second_name'
    __academic_d = 'academic_degree'
    __direction = 'scientific_direction'
    __place_of_job = 'place_of_job'
    __post = 'post'
    __country = 'country'
    __city = 'city'
    __number = 'number'



    def get(self):
        return super().get(self.__nameTable)

    def delete(self, id):
        super().delete(self.__nameTable, id)

    def add(self):
        first = input("Введите имя: ")
        second = input("Введите фамилию ")
        academic_d = input("Введите акадаме степень: ")
        direction = input ("Введите направление: ")
        place_of_job = input("Введите место работы: ")
        post = input("Введите должность: ")
        country = input("Введите страну: ")
        city = input("Введите город: ")
        number = input("Введите номер: ")

        str = f"{self.__first}, {self.__second}, {self.__academic_d}, {self.__direction}, {self.__place_of_job}, {self.__post}, {self.__country}, {self.__city}, {self.__number}"
        super().add(self.__nameTable, str, first, second, academic_d, direction, place_of_job, post, country, city, number)

    def update(self):
        id = input("Введите id записи которую надо обновить ")
        field = input("Введите переменную записи которую надо обновить ")
        values = input("Введите значение записи которую надо обновить ")
        super().update(self.__nameTable, id, field, values)

    def getOneField(self, field):
        return super().getOneField(self.__nameTable, field)


    def selectHumanCost(self):
        return super().selectHumanCost()

    def cost_pay(self):
        date = input("Введите дату заключенную в одиночные ковычки: ")
        return super().cost_pay(date)
    def tezisdCity(self):
        nameCity = input("Введите название города в одиночных ковычках: ")
        return super().tezisdCity(nameCity)
