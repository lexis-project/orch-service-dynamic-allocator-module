from marshmallow import Schema, fields, ValidationError


def validate_quantity(n):
    if n <= 0:
        raise ValidationError("Quantity must be greater than 0.")


def validate_list(n):
    if len(n) <= 0:
        raise ValidationError("List can't be empty")


class LocationSchema(Schema):
    locations = fields.List(fields.String(), validate=validate_list, required=True)
    size = fields.Integer(validate=validate_quantity, required=True)
    numberOfFiles = fields.Integer(validate=validate_quantity, required=True)


class BaseSchema(Schema):
    number = fields.Integer(validate=validate_quantity, required=True)
    project = fields.String(required=True)
    storage_inputs = fields.List(fields.Nested(LocationSchema), required=True)
    original_request_id = fields.String(required=True)


class HPCSchema(BaseSchema):
    max_walltime = fields.Integer(validate=validate_quantity, required=True)
    max_cores = fields.Integer(validate=validate_quantity, required=True)
    taskName = fields.String(required=True)


class CloudSchema(BaseSchema):
    os_version = fields.String(required=True)
    vCPU = fields.Integer(validate=validate_quantity, required=True)
    mem = fields.Integer(validate=validate_quantity, required=True)
    disk = fields.Integer(validate=validate_quantity, required=True)
    inst = fields.Integer(validate=validate_quantity, required=True)
