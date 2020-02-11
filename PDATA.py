#!/usr/bin/env python

'''
__author__: Valdir Junior
'''

import re
from unicodedata import normalize
import csv
import requests
import pymysql
from xlsxwriter.workbook import Workbook
import datetime
import pandas as pd
import math

class SELENA:

	con = None

	def __init__(self):
		self.con = pymysql.connect(user='app', password='root', db='PDATA',cursorclass=pymysql.cursors.DictCursor)


	def cleanBo(self, historic):
	    historic = normalize('NFD', historic).encode('ASCII', 'ignore').decode('ASCII')
	    return historic.strip('*,.- ').upper()

	def openAudited(self, path):
		with open(path, newline='', encoding='latin1') as f:
		    reader = csv.reader(f, delimiter=';')
		    rg = []

		    city = ''
		    state = ''

		    for row in reader:
		    	ln = {}
		    	ln['key'] = row[0]
		    	ln['year'] = row[1]
		    	
		    	if row[3] != city or row[2] != state:
		    		city = row[3]
		    		state = row[2]
		    		city_info = requests.get('http://localhost:5000/api/0.1/info/'+state+'/'+city).json() #conect with BRASA (API for brazilian states and cities)

		    		if 'error' not in city_info:
		    			ln['city'] = city_info['info']['id']
		    	else:
		    		ln['city'] = city_info['info']['id']
		    	
		    	ln['event'] = row[7]
		    	ln['classification'] = row[10]
		    	ln['historic'] = row[8]
		    	rg.append(ln)
		
		return rg

	def getIdClassification(self, classification):
		cursor = self.con.cursor()

		query = "SELECT id FROM classification WHERE initials = %s"
		cursor.execute(query,(classification))
		data = cursor.fetchone()

		if data is not None:
			return data['id']

	def getIdEvent(self, event):
		cursor = self.con.cursor()

		query = "SELECT id FROM typeEvent WHERE UPPER(description) = UPPER(%s)"
		cursor.execute(query,(event))
		data = cursor.fetchone()

		if data is not None:
			return data['id']

	def insertAudit(self, event):
		
		try:
			cursor = self.con.cursor()

			query = "INSERT INTO audit (boKey, year, idTypeEvent, IdClassification, idBrazilianCity, historic) VALUES (%s, %s, %s, %s, %s, %s)"
			cursor.execute(query, (event['key'], event['year'], event['event'], event['classification'], event['city'], event['historic']))
			self.con.commit()
		finally:
			return True

	def getAmountEc(self):
		ec = {}
		ec['1'] = input('Informe a quantidade EC1: ')
		ec['2'] = input('Informe a quantidade EC2: ')
		ec['3'] = input('Informe a quantidade EC3: ')

		return ec

	def writeHeader(self, sheet):

		sheet.write('A1', 'Chave')			
		sheet.write('B1', 'Ano')
		sheet.write('C1', 'UF')
		sheet.write('D1', 'Cidade')
		sheet.write('E1', 'Bairro')
		sheet.write('F1', 'Logradouro')
		sheet.write('G1', 'Classificação')
		sheet.write('H1', 'Evento')
		sheet.write('I1', 'Histórico')
		sheet.write('J1', 'Analista')
		sheet.write('K1', 'Classificao Para')
		sheet.write('L1', 'Motivo da Reclassificação')	

		return sheet

	def generateAudit(self, amountEc):
		#get date in us format to create the file
		date = str(datetime.datetime.now()).split()[0]
		file = date+'_nao_auditado_ec1_ec2_ec3.xlsx'
		workbook = Workbook(file)
		sheet = workbook.add_worksheet()
		sheet = self.writeHeader(sheet)

		con = pymysql.connect(user='app', password='root')
		try:
			cursor = con.cursor()

			print(amountEc)

			start = 1

			for key in amountEc:
				print('\nBuscando '+amountEc[key]+' Registros de EC'+key+' ...')
				query = "SELECT 'Chave', 'Ano', 'UF', 'Cidade', 'Bairro', 'Logradouro', 'Classificação', 'Evento', 'Histórico' UNION ALL \
						SELECT  concat(registros.SP.ID_DELEGACIA,'-',registros.SP.ANO_BO,'-',registros.SP.NUM_BO) as Chave,\
						        prevcrime.bo.yearBo as Ano, registros.SP.ID_UF as UF, registros.SP.CIDADE as Cidade, registros.SP.BAIRRO as Bairro, registros.SP.LOGRADOURO as Logradouro,\
						        concat('EC',prevcrime.dataclassification.idTypeEc) as Classificação,\
						        CASE\
						          WHEN prevcrime.bo.idTypeCrime = 1 THEN 'ROUBO'\
						          WHEN  prevcrime.bo.idTypeCrime = 2 THEN 'FURTO'\
						          ELSE NULL\
						        END as Evento,\
						        trim(prevcrime.bo.historicBo) as Histórico\
						FROM registros.SP\
						  LEFT JOIN prevcrime.bo ON (prevcrime.bo.yearBo = registros.SP.ANO_BO AND prevcrime.bo.idBo = NUM_BO AND prevcrime.bo.idPoliceStation = registros.SP.ID_DELEGACIA)\
						  LEFT JOIN prevcrime.dataclassification ON (prevcrime.bo.id = prevcrime.dataclassification.idBo)\
						WHERE prevcrime.dataclassification.idTypeEc = "+key+" AND registros.SP.enviado_auditoria = 0 ORDER BY rand() LIMIT "+amountEc[key]+";"

				cursor.execute(query)

				print('\nEscrevendo EC%s...' % key)		

				for r, row in enumerate(cursor.fetchall(), start=start):
				    for c, col in enumerate(row):
				        sheet.write(r, c, col)

				start = start+int(amountEc[key])
			
		finally:
			workbook.close()
			con.close()	

	def validateAmoutEc(self, amount, idEc):
		try:
			cursor = self.con.cursor()
			
			query = "SELECT COUNT(1) as amount FROM audit WHERE IdClassification = %s"
			cursor.execute(query, (idEc))
			dbAmount = cursor.fetchone()['amount']

			if dbAmount < amount:
				amount = dbAmount

		finally:
			return amount

	def generateMLTraining(self, amount):
		try:
			cursor = self.con.cursor()

			query = "SELECT id, initials FROM classification"
			cursor.execute(query)
			ecs = cursor.fetchall()

			data = []

			for ec in ecs:
				print (ec['initials'])

				ec['amount'] = self.validateAmoutEc(amount, ec['id'])

				query = "SELECT DISTINCT cl.initials, ad.historic FROM audit ad LEFT JOIN classification cl ON (cl.id = ad.IdClassification) WHERE cl.initials = %s ORDER BY RAND() LIMIT %s"
				cursor.execute(query, (ec['initials'], ec['amount']))
				con.commit()

				data += cursor.fetchall()

			for dt in data:
				dt['historic'] = self.cleanBo(dt['historic'])

			df = pd.DataFrame(data, columns = ['initials','historic'])
			df.to_csv('data/EC_training_SP.csv', sep=';', line_terminator='\n',header=False, index=False)

		finally:
			return True

	def generateMLTrainingRO(self, amount):
		con = pymysql.connect(user='app', password='root', db='registros',cursorclass=pymysql.cursors.DictCursor) #conect with external database for getting registers for training our ML
		try:
			cursor = con.cursor()
			ecs = ['1','2','3']

			data = []

			for ec in ecs:
				query = "SELECT DISTINCT CONCAT('EC', ec_auditoria) as initials, historico FROM ro WHERE ec_auditoria = %s ORDER BY RAND() LIMIT %s"
				cursor.execute(query, (ec, 2000))
				con.commit()

				data += cursor.fetchall()

			for dt in data:
				dt['historico'] = self.cleanBo(dt['historico'])

			df = pd.DataFrame(data, columns = ['initials','historico'])
			df.to_csv('data/EC_training_RO.csv', sep=';', line_terminator='\n',header=False, index=False)

		finally:
			return True

	def generateMLTesting(self, amount):
		try:
			cursor = self.con.cursor()

			query = "SELECT id, initials FROM classification"
			cursor.execute(query)
			ecs = cursor.fetchall()

			data = []

			for ec in ecs:
				print (ec['initials'])

				ec['amount'] = self.validateAmoutEc(amount, ec['id'])

				query = "SELECT DISTINCT cl.initials, ad.historic FROM audit ad LEFT JOIN classification cl ON (cl.id = ad.IdClassification) WHERE cl.initials = %s ORDER BY RAND() LIMIT %s"
				cursor.execute(query, (ec['initials'], ec['amount']))
				# con.commit()

				data += cursor.fetchall()

			for dt in data:
				dt['historic'] = self.cleanBo(dt['historic'])

			df = pd.DataFrame(data, columns = ['initials','historic'])
			df.to_csv('data/EC_testing.csv', sep=';', line_terminator='\n',header=False, index=False)

		finally:
			return True

	def prepareClassification(self, year):
		con = pymysql.connect(user='app', password='root', db='registros',cursorclass=pymysql.cursors.DictCursor) #conect with external database for getting registers for classification by ML
		try:
			cursor = con.cursor()
			print('Contanto Registros...')
			query = 'SELECT COUNT(1) AS TOTAL FROM ro WHERE ano = %s'
			cursor.execute(query, (year))

			amount = cursor.fetchone()['TOTAL']
			tparts = math.ceil(float(amount)/30000)
			part = 1
			previous = 0

			while part <= tparts:
				print('Buscando Registros...')
				query = 'SELECT nr_ocorr, ano, COALESCE(historico, "") as historico FROM ro WHERE ano = %s LIMIT %s, %s'
				cursor.execute(query, (year, previous*30000, 30000))

				pdata = cursor.fetchall()

				for dt in pdata:
					dt['historico'] = self.cleanBo(dt['historico'])

				print('Escrevendo Registros no Arquivo...')
				df = pd.DataFrame(pdata, columns=['nr_ocorr','ano','historico'])
				df.to_csv('data/EC_classify_ro_'+year+'_'+str(part)+'_n.csv', sep=';', line_terminator='\n', header=False, index=False)

				previous = int(part)
				part+=1

			# for dt in data:
			# 	dt['HISTORICO_BO'] = self.cleanBo(dt['HISTORICO_BO'])
			# print('Escrevendo Registros no Arquivo...')
			# df = pd.DataFrame(data, columns=['nr_ocorr','ano','historico'])
			# df.to_csv('data/EC_classify_ro_'+year+'.csv', sep=';', line_terminator='\n', header=False, index=False)

		finally:
			con.close()



	def showMenu(self):
		print (30 * "-" + "AÇÕES" + 30 * "-")
		print ("1 - Gerar Planilha de Auditoria")
		print ("2 - Importa Dados Auditados")
		print ("3 - Gerar Treinamento para ML")
		print ("4 - Gerar Teste para ML")
		print ("5 - Registros para Classificação/ML")
		print ("0 - Sair")
		print (65 * "-")

	def main(self):
		
		loop = True

		while loop:
			self.showMenu()
			choice = input("Escolha a ação pelo número: ")
			
			if choice == '1':
				amount = self.getAmountEc()
				self.generateAudit(amount)
			elif choice == '2':
				path = input("Caminho do arquivo: ")
				data = self.openAudited(path)

				for reg in data:
					reg['classification'] = self.getIdClassification(reg['classification'])
					reg['event'] = self.getIdEvent(reg['event'])

					if reg['classification'] is not None:
						self.insertAudit(reg)
			elif choice == '3':
				self.generateMLTrainingRO(2000)
			elif choice == '4':
				self.generateMLTesting(1000)
			elif choice == '5':
				year = input("Ano para classificação: ")
				self.prepareClassification(year)
			elif choice == '0':
				self.con.close()
				loop = False

if __name__ == "__main__":
	sl = SELENA()
	sl.main()