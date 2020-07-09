import pandas as pd


# import pyodbc


class FactsetPeople():
    def __init__(self):
        pass

    def miraqle_data_pull(self):
        stmt = """
        select  
        PP.ID as 'miraqleID'
        PP.GivenNames + ' ' + PP.Surname as 'Name',
        PE.Address as 'Email',
        CC.Name as 'Company',
        DL.Name as 'Location'


        from person.person PP
        left join person.email PE
        on PE.PersonID = PP.ID
        inner join person.personcompany PPC
        on PPC.personID = PP.ID
        inner join company.company CC
        on CC.ID = PPC.CompanyID
        inner join definition.location DL
        on DL.ID = CC.LocationID

        where PP.Ispublished = 1
        and PP.IsArchived = 0
        and CC.Ispublished = 1
        """
        try:
            with pyodbc.connect('peepeepoopoo') as conn:
                self.miraqle_data = pd.read_sql(stmt, conn)
        except:
            self.miraqle_data = pd.DataFrame(columns=['miraqleID', 'Name', 'Email', 'Company', 'Location'])

    def factset_data_pull(self):
        try:
            self.factset_data = pd.read_excel('factsetpeople.xlsx')
        except:
            self.factset_data = pd.DataFrame(columns=['personID', 'Name', 'Email', 'Company', 'Location'])


if __name__ == '__main__':
    factset_people = FactsetPeople()

    factset_people.miraqle_data_pull()

    factset_people.factset_data_pull()

    print(factset_people.miraqle_data)

