from mongoengine import connect, Document, StringField, BooleanField

connect(
    db="hw08",
    host="mongodb+srv://milvus:Milvus@milvus.hmkzt.mongodb.net/?retryWrites=true&w=majority&appName=Milvus",
)


class Contact(Document):
    full_name = StringField(required=True, unique=True)
    email = StringField(required=True, unique=True)
    message_sent = BooleanField(default=False)
    phone= StringField()
    preferred_contact_method = StringField(choices=["email", "sms"], default="email")