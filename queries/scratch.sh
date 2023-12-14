db.HotelData.aggregate([
    {
        $match: {
            $expr: {
                $and: [
                    { $gte: [{ $toDouble: "$Age" }, 20] },
                    { $lte: [{ $toDouble: "$Age" }, 40] }
                ]
            }
        }
    },
    {
        $addFields: {
            review_classification: {
                $cond: {
                    if: {
                        $regexMatch: {
                            input: "$customer_review",
                            regex: "\\b(good|great|nice)\\b",
                            options: "i"
                        }
                    },
                    then: "Positive",
                    else: {
                        $cond: {
                            if: {
                                $regexMatch: {
                                    input: "$customer_review",
                                    regex: "\\b(bad|never|worst)\\b",
                                    options: "i"
                                }
                            },
                            then: "Negative",
                            else: "Neutral"
                        }
                    }
                }
            }
        }
    },
    {
        $group: {
            _id: "$Nationality",
            pos_rev: { $sum: { $cond: [{ $eq: ["$review_classification", "Positive"] }, 1, 0] } },
            neg_rev: { $sum: { $cond: [{ $eq: ["$review_classification", "Negative"] }, 1, 0] } },
            neutral_rev: { $sum: { $cond: [{ $eq: ["$review_classification", "Neutral"] }, 1, 0] } }

        }
    }
]);
