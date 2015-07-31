// Boilerplate JavaScript that is automatically included for the eval command.

// Count elements in pileup satisfying an element => bool predicate.
function pileupCount(pileupInstance, f) {
    return pileupGroupCount(
        pileupInstance,
        function(v) {if (f(v)) return true; else return false;})[true];
}

function pileupGroupCount(pileupInstance, f) {
    var elements = pileup(pileupInstance).elements();
    var result = {};
    for (var i = 0 ; i < elements.length() ; i++) {
        var key = f(elements.apply(i));
        if (key in result)
            result[key]++;
        else
            result[key] = 1;
    }
    return result;
}

// Return a pileup, given either the index of the pileup (int) or the label (string).
// If another object is passed, return that object.
function pileup(value) {
    if (typeof value === 'undefined' || value === '')
        value = 0;

    if (typeof value === 'string')
        return _by_label.apply(value);
    else if (typeof value === 'number')
        return _pileups.apply(value);
    else
        return value;
}
// End boilerplate
