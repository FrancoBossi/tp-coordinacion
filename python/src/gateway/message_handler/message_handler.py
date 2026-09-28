from common import message_protocol
import uuid


class MessageHandler:

    def __init__(self):
        # Identifica una consulta completa sin modificar el protocolo externo.
        self.request_id = uuid.uuid4().hex
        self.record_count = 0
    
    def serialize_data_message(self, message):
        #Agrega la identidad de la consulta al mensaje interno de datos
        [fruit, amount] = message
        self.record_count += 1
        return message_protocol.internal.serialize(
            [self.request_id, "DATA", fruit, amount]
        )

    def serialize_eof_message(self, message):
        #Marca el fin de datos de esta consulta dentro del pipeline
        return message_protocol.internal.serialize(
            [self.request_id, "EOF", self.record_count]
        )

    def deserialize_result_message(self, message):
        #Acepta unicamente resultados pertenecientes a esta consulta
        fields = message_protocol.internal.deserialize(message)
        if len(fields) != 3 or fields[0] != self.request_id:
            return []
        if fields[1] != "FINAL_TOP":
            return []
        return fields[2]
