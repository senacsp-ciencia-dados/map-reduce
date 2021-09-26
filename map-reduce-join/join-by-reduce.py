from mrjob.job import MRJob

# https://pt.slideshare.net/shalishvj/map-reduce-joins-31519757

# usuarios.csv
# usuario_id,departamento_id,nome

# departamentos.csv
# departamento_id,nome

class JoinByReduce(MRJob):
  def mapper(self, _, linha):
    dados = linha.rstrip("\n").split(",")

    if len(dados) == 3: # usuarios
      departamento_id = dados[2]
      yield departamento_id, {"tipo":"usuario", "dados": dados}
    elif len(dados) == 2: #departamentos
      departamento_id = dados[0]
      yield departamento_id, {"tipo":"departamento", "dados": dados}

  def reducer(self, chave, valores):
    usuarios = []
    departamento = None

    for valor in valores:
      if valor["tipo"] == "usuario":
        usuarios.append(valor["dados"])
      elif valor["tipo"] == "departamento":
        departamento = valor["dados"]

    for usuario in usuarios:
      usuario.append(departamento[1])
      yield usuario[0], usuario

if __name__ == '__main__':
  JoinByReduce.run()