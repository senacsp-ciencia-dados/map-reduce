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
      yield dados[2], {"dados": dados, "tipo": "usuario"}
    elif len(dados) == 2: # departamentos
      yield dados[0], {"dados": dados, "tipo": "departamento"}
    else:
      raise Exception("Tabela errada")

  def reduce(self, departamento_id, payloads):
    #yield departamento_id, payloads

    departamento = None
    usuarios = []
    for payload in payloads:
      if payload["tipo"] == "departamento":
        departamento = payload["dados"]
      elif payload["tipo"] == "usuario":
        usuarios.append(payload["dados"])

    for usuario in usuarios:
    #   usuario.append(departamento[1])
      yield usuario[0], usuario

if __name__ == '__main__':
  JoinByReduce.run()