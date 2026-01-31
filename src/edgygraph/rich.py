from pydantic import Field, BaseModel

class RichReprMixin(BaseModel):
    """
    Mixin to limit the length of values in the rich representation of a Pydantic model.
    This is useful for large objects that should not be displayed in their entirety in the rich representation.

    Set MAX_CHARS_PER_VALUE to the maximum number of characters to display for each value.
    If a value exceeds this limit, it will be displayed as ```<object of length: {len(str(value))}>```.
    """

    MAX_CHARS_PER_VALUE: int = Field(default=2000, exclude=True)

    def __rich_repr__(self):

        for name, field_info in self.__class__.model_fields.items():
            
            if field_info.exclude:
                continue

            value = getattr(self, name)
            length = len(str(value))

            if length > self.MAX_CHARS_PER_VALUE:
                yield name, f"<object of length: {len(str(value))}>"
            else:
                yield name, value