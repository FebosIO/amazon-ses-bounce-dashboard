export function sunMinutesToDateFromISO(fecha, minutosASumar, returnAsDate = false) {
    const fechaOriginal = new Date(fecha);
    const tiempoActualizado = fechaOriginal.getTime() + minutosASumar * 60000;
    return returnAsDate ? new Date(tiempoActualizado) : tiempoActualizado
}

export function toEpoch(date) {
    return milisegundosToEpoch(date.getTime());
}

export function milisegundosToEpoch(date) {
    return Math.floor(date / 1000);
}