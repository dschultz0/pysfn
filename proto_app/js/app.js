

exports.handler = async (event) => {
    const value = event.strValue
    const optParam = event.optParam ? event.optParam : "defaultValue"
    return {
        available: true,
        optParam: optParam,
        resultURI: "s3://mybucket/XXXXX.pdf"
    }
}
