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
            self.miraqle_data = pd.DataFrame([[1,'aaaa bbbb','ab@gmail.com','ABC Group','NY']]
                                             ,columns=['miraqleID', 'Name', 'Email', 'Company', 'Location'])

    def factset_data_pull(self):
        try:
            self.factset_data = pd.read_excel('factsetpeople.xlsx')
        except:
            self.factset_data = pd.DataFrame([[1,'aaaa bbbb','ab@gmail.com','ABC Group','NY']]
                                             ,columns=['personID', 'Name', 'Email', 'Company', 'Location'])

    def subber(self):
        pass

    def name_process(self):

        for x in [self.miraqle_data,self.factset_data]:
            x['Name'] = x['Name'].str.strip()
            x['FirstName'] = x['Name'].str.split(' ').str[0]
            x['LastName'] = x['Name'].str.split(' ').str[-1]


    def match_me_daddy(self):
        mdf = self.miraqle_data.copy()
        fdf = self.factset_data.copy()

        matchers = [['Email'],
                    ['FirstName','LastName', 'Company', 'Location'],
                    ['FirstName','LastName', 'Company'],
                    # ['FirstName','LastName', 'Company', 'Location'],
                    # ['FirstName','LastName', 'Company', 'Location'],
                    # ['FirstName','LastName', 'Company', 'Location'],
                    # ['FirstName','LastName', 'Company', 'Location']
                    ]

        for x in matchers:
            mcols = x.append('miraqleID')
            fcols = x.append('personID')
            results = mdf[mcols].merge(fdf[fcols], on=x, how='inner')

        print('hhhhhhhhhhhhhhh')
        print(results)
        print(results.columns)

if __name__ == '__main__':
    factset_people = FactsetPeople()

    factset_people.miraqle_data_pull()

    factset_people.factset_data_pull()

    factset_people.subber()

    factset_people.name_process()

    factset_people.match_me_daddy()

    print(factset_people.miraqle_data)



