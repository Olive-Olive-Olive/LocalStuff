import pandas as pd


# import pyodbc


class FactsetPeople():
    def __init__(self):
        pass

    def pull_match_data(self):
        try:
            self.match_data = pd.read_excel('people matches.xlsx')
        except:
            self.match_data = pd.DataFrame([[1,11],[4,41],[5,61]],columns=['miraqleID','personID'])




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
            dummydata = [[1,'aaaa bbbb','ab@gmail.com','ABC Group','NY'],
                         [2,'djhmj mdt','aFGJb@gmail.com','K,D,K Group','NYL'],
                         [3,'dghm mdtm,','abZDFH@gmail.com','YUDF Group','NYU'],
                         [4,'aadgmaa zawrh','aDZFGb@gmail.com','NGHM Group','NYD'],
                         [5,'dmghm yhbe','aZDFHb@gmail.com','RTDM Group','NYG'],
                         [6,'zsrmk stjmy','abZDFG@gmail.com','XFGMH Group','NYG'],
                         [7,'EGBzsrmk RsTGJNtjmy','aJRbZDFG@gmail.com','XFGMH JRGroup','NYJG']]
            self.miraqle_data = pd.DataFrame(dummydata,columns=['miraqleID', 'Name', 'Email', 'Company', 'Location'])

    def factset_data_pull(self):
        try:
            self.factset_data = pd.read_excel('factsetpeople.xlsx')
        except:
            dummydata = [[11,'aaaa bbbb','ab@gmail.com','ABC Group','NY'],
                         [21,'djhmj mdt','aFGVSDJb@gmail.com','K,D,K Group','NYL'],
                         [31,'dghm mdtm,','abZDFHDVS@gmail.com','YUDF Group','NYCU'],
                         [41,'aDSadgmaa zawrh','aDZFGSVSb@gmail.com','NGHM Group','NYD'],
                         [51,'dmghm yhbCSe','aZDBDFHb@gmail.com','RTDM Group','NYG'],
                         [61,'zsrGYUDSDmk stjmERy','abZDDBNMFG@gmail.com','XFGMH Group2','NYG'],
                         [71,'EGBzTU,srmk RsTGJNTU,tjmy','aJRbZDT,TFG@gmail.com','XFGT,MH JRGU,roup','NYUJG']]
            self.factset_data = pd.DataFrame(dummydata,columns=['personID', 'Name', 'Email', 'Company', 'Location'])

    def subber(self):
        pass

    def name_process(self):

        for x in [self.miraqle_data,self.factset_data]:
            x['Name'] = x['Name'].str.strip()
            x['FirstName'] = x['Name'].str.split(' ').str[0]
            x['LastName'] = x['Name'].str.split(' ').str[-1]
            x['FirstInitial'] = x['FirstName'].str[0]
            x['LastInitial'] = x['LastName'].str[0]


    def match_me_daddy(self):
        mdf = self.miraqle_data.copy()
        fdf = self.factset_data.copy()

        matchers = {'Email':['Email'],
                    'FirstLastCoLoc':['FirstName','LastName', 'Company', 'Location'],
                    'FirstLastCo':['FirstName','LastName', 'Company'],
                    'FirstInLastCoLoc':['FirstInitial','LastName', 'Company', 'Location'],
                    'FirstLastInCoLoc':['FirstName','LastInitial', 'Company', 'Location'],
                    'FirstInLastInCoLoc':['FirstInitial','LastInitial', 'Company', 'Location']
        }

        results = pd.DataFrame()

        for y in matchers:
            if 'Company' in matchers[y]:
                mdf['CompanyTrunc'] = mdf['Company']
                fdf['CompanyTrunc'] = fdf['Company']
                matchers[y] = ['CompanyTrunc' if x == 'Company' else x for x in matchers[y]]

            mcols = matchers[y].copy()
            mcols.append('miraqleID')
            fcols = ['personID' if x == 'miraqleID' else x for x in mcols]

            if 'CompanyTrunc' in mcols:
                for z in range(40,7,-1):
                    mdf['CompanyTrunc'] = mdf['CompanyTrunc'].str[:z]
                    fdf['CompanyTrunc'] = fdf['CompanyTrunc'].str[:z]
                    dummy = mdf[mcols].merge(fdf[fcols], on=matchers[y], how='inner')
                    dummy['MatchType'] = y + str(z)
                    results = pd.concat([results,dummy[['miraqleID','personID','MatchType']]])
            else:
                dummy = mdf[mcols].merge(fdf[fcols], on=matchers[y], how='inner')
                dummy['MatchType'] = y
                results = pd.concat([results,dummy[['miraqleID','personID','MatchType']]])

        self.new_match_results = results.drop_duplicates(subset=['miraqleID']).reset_index(drop=True).copy()
        print(self.new_match_results)

    def process_matches(self):
        df = self.new_match_results.copy()
        matchdf = self.match_data.copy()
        matchdf['filter'] = 'matched'

        mergedf = df.merge(matchdf, on=['miraqleID', 'personID'], how='left')
        newmatches = mergedf[mergedf['filter'] != 'matched'].copy()
        self.new_matches = newmatches[df.columns].copy()

        matchdf.columns = ['oldpersonID' if x == 'personID' else x for x in matchdf.columns]
        mergedf = df.merge(matchdf, on=['miraqleID'], how='left').dropna()
        mergedf['filter'] = mergedf.apply(lambda x: 'yes' if x['personID'] != x['oldpersonID'] else '', axis=1)
        self.conflict_matches = mergedf[mergedf['filter'] == 'yes']
        self.conflict_matches = self.conflict_matches[['miraqleID','personID','oldpersonID','MatchType']]


        print(df.columns)
        print(matchdf)
        print(self.new_matches)
        print(self.conflict_matches)



if __name__ == '__main__':
    factset_people = FactsetPeople()

    factset_people.pull_match_data()

    factset_people.miraqle_data_pull()

    factset_people.factset_data_pull()

    factset_people.subber()

    factset_people.name_process()

    factset_people.match_me_daddy()

    factset_people.process_matches()







