// Boilerplate JavaScript that is automatically included for the eval command.

// Return a pileup, given either the index of the pileup (int) or the label (string).
function pileup(value) {
    if (typeof value === 'string')
        return _by_label.apply(value);
    else if (typeof value === 'undefined')
        value = 0
    return _pileups.apply(value);
}
// End boilerplate
