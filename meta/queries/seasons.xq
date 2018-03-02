jsoniq version "1.0";

import module namespace file = "http://expath.org/ns/file";

let $maps := jn:parse-json(file:read-text("./meta/data/seasons.json"))."ContentItems"

return [
    for $map in $maps[]
    where $map."Type" = "HW2Season"
    return [
        $map."View"."Identity",
        $map."View"."HW2Season"."DisplayInfo"."View"."HW2SeasonDisplayInfo"."Name",
        $map."View"."HW2Season"."StartDate",
        $map."View"."HW2Season"."Image"."View"."Media"."MediaUrl",
        $map."View"."HW2Season"."Image4K"."View"."Media"."MediaUrl",
        [
            for $playlist in $map."View"."HW2Season"."Playlists"[]
            return $playlist."Identity"
        ]
    ]
]
