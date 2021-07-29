from marshmallow import Schema, fields, EXCLUDE, ValidationError

def validate_quantity(n):
    if n <= 0:
        raise ValidationError("Quantity must be greater than 0.")

def validate_list(n):
    if len(n) <= 0:
        raise ValidationError("List can't be empty")

class LocationSchema(Schema):
    locations = fields.List(fields.String(),validate=validate_list, required=True)
    size = fields.Integer(validate=validate_quantity, required=True)
    numberOfFiles = fields.Integer(validate=validate_quantity, required=True)

class BaseSchema(Schema):
    number = fields.Integer(required=True)
    project = fields.String(required=True)
    storage_inputs = fields.List(fields.Nested(LocationSchema), required=True)

class HPCSchema(BaseSchema):
    max_walltime = fields.Integer(required=True)
    max_cores = fields.Integer(required=True)
    taskName = fields.String(required=True)
    resubmit = fields.Boolean(required=False)

class CloudSchema(BaseSchema):
    os_version = fields.String(required=True)
    vCPU = fields.Integer(required=True)
    mem = fields.Integer(required=True)
    disk = fields.Integer(required=True)
    inst = fields.Integer(required=True)