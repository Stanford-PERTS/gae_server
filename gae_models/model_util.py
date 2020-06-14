def reverse_order_str(order_str):
    """Given some ordering, possibly already negative, reverse it."""
    # Historical note on why we abstract orderings as strings:

    # NDB makes it really hard to reverse an arbitrary ordering.
    # Specifically, Property.__neg__() returns a PropertyOrder, not
    # a Property, so you can't do Property.__neg__().__neg__()
    # This gets around that by reversing the string form.

    if order_str.startswith('-'):
        return order_str[1:]
    else:
        return '-' + order_str
