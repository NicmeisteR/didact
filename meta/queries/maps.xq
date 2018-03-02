jsoniq version "1.0";

import module namespace file = "http://expath.org/ns/file";

let $maps := jn:parse-json(file:read-text("./meta/data/maps.json"))."ContentItems"

return [
    for $map in $maps[]
    where $map."Type" = "HW2Map"
    return [
        $map."View"."Identity",
    ]
]
