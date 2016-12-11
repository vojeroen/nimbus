from nimbus import errors


class ModelActions:
    class Meta:
        model = None
        serializer = None

    def __init__(self):
        self._unique_columns = []
        for column in self.Meta.model.__table__.columns:
            if column.unique:
                self._unique_columns.append(column)

    def create(self, message):
        session = message.session
        model = self.Meta.model
        serializer = self.Meta.serializer(message.payload)
        assert model == serializer.model

        for column in self._unique_columns:
            if column in serializer.validated_data.keys():
                column_count = session.query(model).filter(column == serializer.validated_data[column.name]).count()
                if column_count >= 1:
                    raise errors.InstanceExists('{model_name} with {column_name} "{column_value}" exists'.format(
                        model_name=str(model).strip('>').strip('<').strip("'").split('.')[-1],
                        column_name=column.name,
                        column_value=serializer.validated_data[column.name],
                    ))

        session.add(serializer.instance)
        session.commit()
        return message.generate_response(serializer)

    def retrieve(self, message):
        session = message.session
        model = self.Meta.model

        query = session.query(model)
        for key, value in message.payload.items():
            if key in model.__mapper__.columns:
                column = model.__mapper__.columns.get(key)
                query = query.filter(column == value)
            elif key in model.__mapper__.relationships:
                columns = model.__mapper__.relationships.get(key).local_columns
                assert len(columns) == 1, 'Retrieve action does not support multiple foreign keys'
                for column in columns:
                    query = query.filter(column == value)
            else:
                raise errors.PayloadNotCorrect('{model_name} does not contain the attribute {column_name}'.format(
                    model_name=str(model).strip('>').strip('<').strip("'").split('.')[-1],
                    column_name=key,
                ))

        query_results = query.all()
        serializers = map(lambda instance: self.Meta.serializer(instance), query_results)
        return message.generate_response(serializers)

    @classmethod
    def route_create(cls, message):
        action = cls()
        return action.create(message)

    @classmethod
    def route_retrieve(cls, message):
        action = cls()
        return action.retrieve(message)
