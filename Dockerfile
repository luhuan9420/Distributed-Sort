FROM golang:1.16-alpine

WORKDIR /CSE224

COPY ./ /CSE224/

EXPOSE 8080

RUN cd src && go build -o netsort

ENV SERVER_ID 0
ENV INPUT_FILE_PATH ""
ENV OUTPUT_FILE_PATH ""
ENV CONFIG_FILE_PATH ""

CMD ["sh", "-c", "cd src && ./netsort ${SERVER_ID} ${INPUT_FILE_PATH} ${OUTPUT_FILE_PATH} ${CONFIG_FILE_PATH}"]