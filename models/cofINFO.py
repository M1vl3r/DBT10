from models.model import Model

class confINFO(Model):
    __nameTable = 'confINFO'
    __human_id = 'human_id'
    __role_id = 'role_id'
    __Topic = 'Topic'
    __tezis_id = 'tezis_id'
    __first_invite = 'first_invite'
    __second_invite = 'second_invite'
    __date_pay = 'date_pay'
    __cost_pay = 'cost_pay'
    __date_in = 'date_in'
    __date_on = 'date_on'
    __nees_hostel_id = 'need_hostel_id'




    def get(self):
        return super().get(self.__nameTable)

    def delete(self, id):
        super().delete(self.__nameTable, id)

    # def add(self):
    #     name = input("Введите имя: ")
    #     start_time = input("Введите время старта в формате 1 окт. 2023 г., 12:24:18: ")
    #     finish_time = input("Введите время старта в формате 1 окт. 2023 г., 12:24:18: ")
    #     mountain_id = input ("Введите id горы: ")
    #     str = f"{self.__name}, {self.__start_time}, {self.__finish_time}, {self.__mountain_id}"
    #     super().add(self.__nameTable, str, name, start_time, finish_time, mountain_id)

    def update(self):
        id = input("Введите id записи которую надо обновить ")
        field = input("Введите переменную записи которую надо обновить ")
        values = input("Введите значение записи которую надо обновить ")
        super().update(self.__nameTable, id, field, values)

    def getOneField(self, field):
        return super().getOneField(self.__nameTable, field)

    def countConfINFO(self):
        date = input("Введите дату заключенную в одиночные ковычки: ")
        return super().countConfINFO(date)

    def neededHostelonCity(self):
        nameCity = input("Введите название города для вывода нуждающихся в отеле в одиночных скобках: ")
        return super().neededHostelonCity(nameCity)




