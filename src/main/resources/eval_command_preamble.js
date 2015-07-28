// Boilerplate JavaScript that is automatically included for the eval command.

// This should be replaced at execution time by a scala function.
function _by_label(value) { throw new Error("not overridden.") }
function pileups(value) { throw new Error("not overridden.") }

// Return a pileup, given either the index of the pileup (int) or the label (string).
function pileup(value) {
    if (typeof value === 'string')
        return _by_label.apply(value);
    else if (typeof value === 'undefined')
        value = 0
    return pileups.apply(value);
}

// End boilerplate

